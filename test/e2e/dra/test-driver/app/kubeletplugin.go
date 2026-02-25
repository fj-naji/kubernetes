/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	cgoresource "k8s.io/client-go/kubernetes/typed/resource/v1"
	draclient "k8s.io/dynamic-resource-allocation/client"
	"k8s.io/dynamic-resource-allocation/kubeletplugin"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	"k8s.io/klog/v2"
	drahealthv1alpha1 "k8s.io/kubelet/pkg/apis/dra-health/v1alpha1"
)

type Options struct {
	EnableHealthService bool
}

type DeviceHealthUpdate struct {
	PoolName   string
	DeviceName string
	Health     string
	Message    string
}

type deviceHealthInfo struct {
	status  string
	message string
}

type ExamplePlugin struct {
	drahealthv1alpha1.UnimplementedDRAResourceHealthServer
	stopCh         <-chan struct{}
	logger         klog.Logger
	resourceClient cgoresource.ResourceV1Interface
	d              *kubeletplugin.Helper
	fileOps        FileOperations

	cdiDir     string
	driverName string
	nodeName   string

	// The mutex is needed because there are other goroutines checking the state.
	// Serializing in the gRPC server alone is not enough because writing would
	// race with reading.
	mutex     sync.Mutex
	prepared  map[ClaimID][]kubeletplugin.Device // prepared claims -> result of nodePrepareResource
	gRPCCalls []GRPCCall

	healthMutex       sync.Mutex
	deviceHealth      map[string]deviceHealthInfo
	HealthControlChan chan DeviceHealthUpdate

	blockPrepareResourcesMutex   sync.Mutex
	blockUnprepareResourcesMutex sync.Mutex

	prepareResourcesFailure   error
	failPrepareResourcesMutex sync.Mutex

	unprepareResourcesFailure   error
	failUnprepareResourcesMutex sync.Mutex

	// cancelMainContext is used to cancel an upper-level context.
	// It's called from HandleError if set.
	cancelMainContext context.CancelCauseFunc
}

var _ kubeletplugin.DRAPlugin = &ExamplePlugin{}
var _ drahealthv1alpha1.DRAResourceHealthServer = &ExamplePlugin{}

//nolint:unused
func (ex *ExamplePlugin) mustEmbedUnimplementedDRAResourceHealthServer() {}

type GRPCCall struct {
	// FullMethod is the fully qualified, e.g. /package.service/method.
	FullMethod string

	// Request contains the parameters of the call.
	Request interface{}

	// Response contains the reply of the plugin. It is nil for calls that are in progress.
	Response interface{}

	// Err contains the error return value of the plugin. It is nil for calls that are in progress or succeeded.
	Err error
}

// ClaimID contains both claim name and UID to simplify debugging. The
// namespace is not included because it is random in E2E tests and the UID is
// sufficient to make the ClaimID unique.
type ClaimID struct {
	Name string
	UID  types.UID
}

type Device struct {
	PoolName    string
	DeviceName  string
	RequestName string
	CDIDeviceID string
}

var _ kubeletplugin.DRAPlugin = &ExamplePlugin{}

// getJSONFilePath returns the absolute path where CDI file is/should be.
func (ex *ExamplePlugin) getJSONFilePath(claimUID types.UID, requestName string) string {
	baseRequestRef := resourceclaim.BaseRequestRef(requestName)
	return filepath.Join(ex.cdiDir, fmt.Sprintf("%s-%s-%s.json", ex.driverName, claimUID, baseRequestRef))
}

// FileOperations defines optional callbacks for handling CDI files
// and some other configuration.
type FileOperations struct {
	// Create must overwrite the file.
	Create func(name string, content []byte) error

	// Remove must remove the file. It must not return an error when the
	// file does not exist.
	Remove func(name string) error

	// HandleError is an optional callback for ResourceSlice publishing problems.
	HandleError func(ctx context.Context, err error, msg string)

	// DriverResources provides the information that the driver will use to
	// construct the ResourceSlices that it will publish.
	DriverResources *resourceslice.DriverResources
}

// StartPlugin sets up the servers that are necessary for a DRA kubelet plugin.
func StartPlugin(ctx context.Context, cdiDir, driverName string, kubeClient kubernetes.Interface, nodeName string, fileOps FileOperations, opts ...any) (*ExamplePlugin, error) {
	logger := klog.FromContext(ctx)

	if fileOps.Create == nil {
		fileOps.Create = func(name string, content []byte) error {
			return os.WriteFile(name, content, os.FileMode(0644))
		}
	}
	if fileOps.Remove == nil {
		fileOps.Remove = func(name string) error {
			if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
				return err
			}
			return nil
		}
	}

	publicOpts := []kubeletplugin.Option{
		kubeletplugin.DriverName(driverName),
		kubeletplugin.NodeName(nodeName),
		kubeletplugin.KubeClient(kubeClient),
	}

	testOpts := &options{}
	pluginOpts := &Options{}
	for _, opt := range opts {
		switch typedOpt := opt.(type) {
		case Options:
			*pluginOpts = typedOpt
		case TestOption:
			if err := typedOpt(testOpts); err != nil {
				return nil, fmt.Errorf("apply test option: %w", err)
			}
		case kubeletplugin.Option:
			publicOpts = append(publicOpts, typedOpt)
		default:
			return nil, fmt.Errorf("unexpected option type %T", opt)
		}
	}

	ex := &ExamplePlugin{
		stopCh:            ctx.Done(),
		logger:            logger,
		resourceClient:    draclient.New(kubeClient),
		fileOps:           fileOps,
		cdiDir:            cdiDir,
		driverName:        driverName,
		nodeName:          nodeName,
		prepared:          make(map[ClaimID][]kubeletplugin.Device),
		cancelMainContext: testOpts.cancelMainContext,
		deviceHealth:      make(map[string]deviceHealthInfo),
		HealthControlChan: make(chan DeviceHealthUpdate, 10),
	}

	publicOpts = append(publicOpts,
		kubeletplugin.GRPCInterceptor(ex.recordGRPCCall),
		kubeletplugin.GRPCStreamInterceptor(ex.recordGRPCStream),
	)
	d, err := kubeletplugin.Start(ctx, ex, publicOpts...)
	if err != nil {
		return nil, fmt.Errorf("start kubelet plugin: %w", err)
	}
	ex.d = d

	if fileOps.DriverResources != nil {
		if err := ex.d.PublishResources(ctx, *fileOps.DriverResources); err != nil {
			return nil, fmt.Errorf("start kubelet plugin: publish resources: %w", err)
		}
	}

	// Start the binding conditions controller - must run independently of
    // kubelet calls because the scheduler checks conditions BEFORE binding.
    ex.StartBindingConditionsController(ctx, kubeClient)

	return ex, nil
}

// Stop ensures that all servers are stopped and resources freed.
func (ex *ExamplePlugin) Stop() {
	ex.d.Stop()
}

func (ex *ExamplePlugin) IsRegistered() bool {
	status := ex.d.RegistrationStatus()
	if status == nil {
		return false
	}
	return status.PluginRegistered
}

func (ex *ExamplePlugin) HandleError(ctx context.Context, err error, msg string) {
	if ex.fileOps.HandleError != nil {
		ex.fileOps.HandleError(ctx, err, msg)
		return
	}
	utilruntime.HandleErrorWithContext(ctx, err, msg)
	if ex.cancelMainContext != nil {
		ex.cancelMainContext(err)
	}
}

// BlockNodePrepareResources locks blockPrepareResourcesMutex and returns unlocking function for it
func (ex *ExamplePlugin) BlockNodePrepareResources() func() {
	ex.blockPrepareResourcesMutex.Lock()
	return func() {
		ex.blockPrepareResourcesMutex.Unlock()
	}
}

// BlockNodeUnprepareResources locks blockUnprepareResourcesMutex and returns unlocking function for it
func (ex *ExamplePlugin) BlockNodeUnprepareResources() func() {
	ex.blockUnprepareResourcesMutex.Lock()
	return func() {
		ex.blockUnprepareResourcesMutex.Unlock()
	}
}

// SetNodePrepareResourcesFailureMode sets the failure mode for NodePrepareResources call
// and returns a function to unset the failure mode
func (ex *ExamplePlugin) SetNodePrepareResourcesFailureMode() func() {
	ex.failPrepareResourcesMutex.Lock()
	ex.prepareResourcesFailure = errors.New("simulated PrepareResources failure")
	ex.failPrepareResourcesMutex.Unlock()

	return func() {
		ex.failPrepareResourcesMutex.Lock()
		ex.prepareResourcesFailure = nil
		ex.failPrepareResourcesMutex.Unlock()
	}
}

func (ex *ExamplePlugin) getPrepareResourcesFailure() error {
	ex.failPrepareResourcesMutex.Lock()
	defer ex.failPrepareResourcesMutex.Unlock()
	return ex.prepareResourcesFailure
}

// SetNodeUnprepareResourcesFailureMode sets the failure mode for NodeUnprepareResources call
// and returns a function to unset the failure mode
func (ex *ExamplePlugin) SetNodeUnprepareResourcesFailureMode() func() {
	ex.failUnprepareResourcesMutex.Lock()
	ex.unprepareResourcesFailure = errors.New("simulated UnprepareResources failure")
	ex.failUnprepareResourcesMutex.Unlock()

	return func() {
		ex.failUnprepareResourcesMutex.Lock()
		ex.unprepareResourcesFailure = nil
		ex.failUnprepareResourcesMutex.Unlock()
	}
}

func (ex *ExamplePlugin) getUnprepareResourcesFailure() error {
	ex.failUnprepareResourcesMutex.Lock()
	defer ex.failUnprepareResourcesMutex.Unlock()
	return ex.unprepareResourcesFailure
}

// NodePrepareResource ensures that the CDI file(s) (one per request) for the claim exists. It uses
// a deterministic name to simplify NodeUnprepareResource (no need to remember
// or discover the name) and idempotency (when called again, the file simply
// gets written again).
func (ex *ExamplePlugin) nodePrepareResource(ctx context.Context, claim *resourceapi.ResourceClaim) ([]kubeletplugin.Device, error) {
	logger := klog.FromContext(ctx)

	ex.mutex.Lock()
	defer ex.mutex.Unlock()
	ex.blockPrepareResourcesMutex.Lock()
	defer ex.blockPrepareResourcesMutex.Unlock()

	claimID := ClaimID{Name: claim.Name, UID: claim.UID}
	if result, ok := ex.prepared[claimID]; ok {
		// Idempotent call, nothing to do.
		return result, nil
	}

	var devices []kubeletplugin.Device
	for _, result := range claim.Status.Allocation.Devices.Results {
		// Only handle allocations for the current driver.
		if ex.driverName != result.Driver {
			continue
		}

		baseRequestName := resourceclaim.BaseRequestRef(result.Request)

		// The driver joins all env variables in the order in which
		// they appear in results (last one wins).
		configs := resourceclaim.ConfigForResult(claim.Status.Allocation.Devices.Config, result)
		env := make(map[string]string)
		for i, config := range configs {
			// Only use configs for the current driver.
			if config.Opaque.Driver != ex.driverName {
				continue
			}
			if err := extractParameters(config.Opaque.Parameters, &env, config.Source == resourceapi.AllocationConfigSourceClass); err != nil {
				return nil, fmt.Errorf("parameters in config #%d: %w", i, err)
			}
		}

		// It also sets a claim_<claim name>_<request name>=true env variable.
		// This can be used to identify which devices where mapped into a container.
		claimReqName := "claim_" + claim.Name + "_" + baseRequestName
		claimReqName = regexp.MustCompile(`[^a-zA-Z0-9]`).ReplaceAllString(claimReqName, "_")
		env[claimReqName] = "true"

		deviceName := "claim-" + string(claim.UID) + "-" + baseRequestName
		vendor := ex.driverName
		class := "test"
		cdiDeviceID := vendor + "/" + class + "=" + deviceName

		// CDI wants env variables as set of strings.
		envs := []string{}
		for key, val := range env {
			envs = append(envs, key+"="+val)
		}
		sort.Strings(envs)

		if len(envs) == 0 {
			// CDI does not support empty ContainerEdits. For example,
			// kubelet+crio then fail with:
			//    CDI device injection failed: unresolvable CDI devices ...
			//
			// Inject nothing instead, which is supported by DRA.
			continue
		}

		spec := &spec{
			Version: "0.3.0", // This has to be a version accepted by the runtimes.
			Kind:    vendor + "/" + class,
			// At least one device is required and its entry must have more
			// than just the name.
			Devices: []device{
				{
					Name: deviceName,
					ContainerEdits: containerEdits{
						Env: envs,
					},
				},
			},
		}
		filePath := ex.getJSONFilePath(claim.UID, baseRequestName)
		buffer, err := json.Marshal(spec)
		if err != nil {
			return nil, fmt.Errorf("marshal spec: %w", err)
		}
		if err := ex.fileOps.Create(filePath, buffer); err != nil {
			return nil, fmt.Errorf("failed to write CDI file: %w", err)
		}
		device := kubeletplugin.Device{
			PoolName:     result.Pool,
			DeviceName:   result.Device,
			ShareID:      result.ShareID,
			Requests:     []string{result.Request}, // May also return baseRequestName here.
			CDIDeviceIDs: []string{cdiDeviceID},
		}
		devices = append(devices, device)
	}

	logger.V(3).Info("CDI file(s) created", "devices", devices)
	ex.prepared[claimID] = devices
	if err := ex.updateBindingConditions(ctx, claim); err != nil {
        logger.Error(err, "Failed to update binding conditions", "claim", klog.KObj(claim))
        // Non-fatal: don't fail the prepare because of this
    }
	return devices, nil
}

func extractParameters(parameters runtime.RawExtension, env *map[string]string, admin bool) error {
	if len(parameters.Raw) == 0 {
		return nil
	}
	kind := "user"
	if admin {
		kind = "admin"
	}
	var data map[string]string
	if err := json.Unmarshal(parameters.Raw, &data); err != nil {
		return fmt.Errorf("decoding %s parameters: %w", kind, err)
	}
	if len(data) > 0 && *env == nil {
		*env = make(map[string]string)
	}
	for key, value := range data {
		(*env)[kind+"_"+key] = value
	}
	return nil
}

func (ex *ExamplePlugin) PrepareResourceClaims(ctx context.Context, claims []*resourceapi.ResourceClaim) (map[types.UID]kubeletplugin.PrepareResult, error) {
	if failure := ex.getPrepareResourcesFailure(); failure != nil {
		return nil, failure
	}

	result := make(map[types.UID]kubeletplugin.PrepareResult)
	for _, claim := range claims {
		devices, err := ex.nodePrepareResource(ctx, claim)
		var claimResult kubeletplugin.PrepareResult
		if err != nil {
			claimResult.Err = err
		} else {
			claimResult.Devices = devices
		}
		result[claim.UID] = claimResult
	}
	return result, nil
}

// NodeUnprepareResource removes the CDI file created by
// NodePrepareResource. It's idempotent, therefore it is not an error when that
// file is already gone.
func (ex *ExamplePlugin) nodeUnprepareResource(ctx context.Context, claimRef kubeletplugin.NamespacedObject) error {
	ex.blockUnprepareResourcesMutex.Lock()
	defer ex.blockUnprepareResourcesMutex.Unlock()

	logger := klog.FromContext(ctx)

	claimID := ClaimID{Name: claimRef.Name, UID: claimRef.UID}
	devices, ok := ex.prepared[claimID]
	if !ok {
		// Idempotent call, nothing to do.
		return nil
	}

	for _, device := range devices {
		// In practice we only prepare one, but let's not assume that here.
		for _, request := range device.Requests {
			filePath := ex.getJSONFilePath(claimRef.UID, request)
			if err := ex.fileOps.Remove(filePath); err != nil {
				return fmt.Errorf("error removing CDI file: %w", err)
			}
			logger.V(3).Info("CDI file removed", "path", filePath)
		}
	}

	delete(ex.prepared, claimID)

	return nil
}

func (ex *ExamplePlugin) UnprepareResourceClaims(ctx context.Context, claims []kubeletplugin.NamespacedObject) (map[types.UID]error, error) {
	result := make(map[types.UID]error)

	if failure := ex.getUnprepareResourcesFailure(); failure != nil {
		return nil, failure
	}

	for _, claimRef := range claims {
		err := ex.nodeUnprepareResource(ctx, claimRef)
		result[claimRef.UID] = err
	}
	return result, nil
}

func (ex *ExamplePlugin) GetPreparedResources() []ClaimID {
	ex.mutex.Lock()
	defer ex.mutex.Unlock()
	var prepared []ClaimID
	for claimID := range ex.prepared {
		prepared = append(prepared, claimID)
	}
	return prepared
}

func (ex *ExamplePlugin) recordGRPCCall(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	call := GRPCCall{
		FullMethod: info.FullMethod,
		Request:    req,
	}
	ex.mutex.Lock()
	ex.gRPCCalls = append(ex.gRPCCalls, call)
	index := len(ex.gRPCCalls) - 1
	ex.mutex.Unlock()

	// We don't hold the mutex here to allow concurrent calls.
	call.Response, call.Err = handler(ctx, req)

	ex.mutex.Lock()
	ex.gRPCCalls[index] = call
	ex.mutex.Unlock()

	return call.Response, call.Err
}

func (ex *ExamplePlugin) recordGRPCStream(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	ex.mutex.Lock()
	// Append a new empty GRPCCall struct to get its index.
	ex.gRPCCalls = append(ex.gRPCCalls, GRPCCall{})

	pCall := &ex.gRPCCalls[len(ex.gRPCCalls)-1]

	pCall.FullMethod = info.FullMethod
	ex.mutex.Unlock()

	defer func() {
		ex.mutex.Lock()
		defer ex.mutex.Unlock()
		pCall.Err = err
	}()

	err = handler(srv, stream)
	return err
}

func (ex *ExamplePlugin) GetGRPCCalls() []GRPCCall {
	ex.mutex.Lock()
	defer ex.mutex.Unlock()

	// We must return a new slice, otherwise adding new calls would become
	// visible to the caller. We also need to copy the entries because
	// they get mutated by recordGRPCCall.
	calls := make([]GRPCCall, 0, len(ex.gRPCCalls))
	calls = append(calls, ex.gRPCCalls...)
	return calls
}

// ResetGRPCCalls clears the internal tracking of GRPC calls made to the plugin.
// This is useful in tests to start with a clean slate when verifying plugin
// registration behavior, particularly when testing registration retry scenarios.
func (ex *ExamplePlugin) ResetGRPCCalls() {
	ex.mutex.Lock()
	defer ex.mutex.Unlock()
	ex.gRPCCalls = nil
}

// CountCalls counts GRPC calls with the given method suffix.
func (ex *ExamplePlugin) CountCalls(methodSuffix string) int {
	count := 0
	for _, call := range ex.GetGRPCCalls() {
		if strings.HasSuffix(call.FullMethod, methodSuffix) {
			count += 1
		}
	}
	return count
}

func (ex *ExamplePlugin) UpdateStatus(ctx context.Context, resourceClaim *resourceapi.ResourceClaim) (*resourceapi.ResourceClaim, error) {
	return ex.resourceClient.ResourceClaims(resourceClaim.Namespace).UpdateStatus(ctx, resourceClaim, metav1.UpdateOptions{})
}

// SetGetInfoError sets an error to be returned by the plugin's GetInfo call.
// This can be used in tests to simulate a registration failure scenario,
// allowing verification that the kubelet plugin manager retries registration
// when GetInfo fails.
//
// To restore normal GetInfo behavior, call SetGetInfoError(nil).
func (ex *ExamplePlugin) SetGetInfoError(err error) {
	ex.d.SetGetInfoError(err)
}

func (ex *ExamplePlugin) NodeWatchResources(req *drahealthv1alpha1.NodeWatchResourcesRequest, srv drahealthv1alpha1.DRAResourceHealth_NodeWatchResourcesServer) error {
	logger := klog.FromContext(srv.Context())
	logger.V(3).Info("Starting dynamic NodeWatchResources stream")

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// Send an initial update immediately to report on pre-configured devices.
	if err := ex.sendHealthUpdate(srv); err != nil {
		logger.Error(err, "Failed to send initial health update")
	}

	for {
		select {
		case <-srv.Context().Done():
			logger.V(3).Info("NodeWatchResources stream canceled by kubelet")
			return nil
		case update, ok := <-ex.HealthControlChan:
			if !ok {
				logger.V(3).Info("HealthControlChan closed, exiting NodeWatchResources stream.")
				return nil
			}
			logger.V(3).Info("Received health update from control channel", "update", update)
			ex.healthMutex.Lock()
			key := update.PoolName + "/" + update.DeviceName
			ex.deviceHealth[key] = deviceHealthInfo{
				status:  update.Health,
				message: update.Message,
			}
			ex.healthMutex.Unlock()

			if err := ex.sendHealthUpdate(srv); err != nil {
				logger.Error(err, "Failed to send health update after control message")
			}
		case <-ticker.C:
			if err := ex.sendHealthUpdate(srv); err != nil {
				if srv.Context().Err() != nil {
					logger.V(3).Info("NodeWatchResources stream closed during periodic update, exiting.")
					return nil
				}
				logger.Error(err, "Failed to send periodic health update")
			}
		}
	}
}

// sendHealthUpdate dynamically builds the health report from the current state of the deviceHealth map.
func (ex *ExamplePlugin) sendHealthUpdate(srv drahealthv1alpha1.DRAResourceHealth_NodeWatchResourcesServer) error {
	logger := klog.FromContext(srv.Context())
	healthUpdates := []*drahealthv1alpha1.DeviceHealth{}

	ex.healthMutex.Lock()
	for key, healthInfo := range ex.deviceHealth {
		parts := strings.SplitN(key, "/", 2)
		if len(parts) != 2 {
			continue
		}
		poolName := parts[0]
		deviceName := parts[1]

		var healthEnum drahealthv1alpha1.HealthStatus
		switch healthInfo.status {
		case "Healthy":
			healthEnum = drahealthv1alpha1.HealthStatus_HEALTHY
		case "Unhealthy":
			healthEnum = drahealthv1alpha1.HealthStatus_UNHEALTHY
		default:
			healthEnum = drahealthv1alpha1.HealthStatus_UNKNOWN
		}

		healthUpdates = append(healthUpdates, &drahealthv1alpha1.DeviceHealth{
			Device: &drahealthv1alpha1.DeviceIdentifier{
				PoolName:   poolName,
				DeviceName: deviceName,
			},
			Health:          healthEnum,
			LastUpdatedTime: time.Now().Unix(),
			Message:         healthInfo.message,
		})
	}
	ex.healthMutex.Unlock()

	// Sorting slice to ensure consistent ordering in tests.
	sort.Slice(healthUpdates, func(i, j int) bool {
		if healthUpdates[i].GetDevice().GetPoolName() != healthUpdates[j].GetDevice().GetPoolName() {
			return healthUpdates[i].GetDevice().GetPoolName() < healthUpdates[j].GetDevice().GetPoolName()
		}
		return healthUpdates[i].GetDevice().GetDeviceName() < healthUpdates[j].GetDevice().GetDeviceName()
	})

	resp := &drahealthv1alpha1.NodeWatchResourcesResponse{Devices: healthUpdates}
	logger.V(5).Info("Test driver sending health update", "response", resp)
	return srv.Send(resp)
}

// StartBindingConditionsController starts a goroutine that watches ResourceClaims
// and sets binding conditions as soon as they are allocated, before kubelet
// calls NodePrepareResources. This is necessary because the scheduler waits
// for binding conditions to be set before binding the pod to the node.
func (ex *ExamplePlugin) StartBindingConditionsController(ctx context.Context, kubeClient kubernetes.Interface) {
    logger := klog.FromContext(ctx)
    logger.V(3).Info("Starting binding conditions controller")

    go func() {
        ticker := time.NewTicker(200 * time.Millisecond)
        defer ticker.Stop()

        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                if err := ex.reconcileAllClaimBindingConditions(ctx, kubeClient); err != nil {
                    logger.Error(err, "Failed to reconcile binding conditions")
                }
            }
        }
    }()
}

// reconcileAllClaimBindingConditions finds all allocated ResourceClaims
// for our driver and sets binding conditions based on the device scenario.
func (ex *ExamplePlugin) reconcileAllClaimBindingConditions(ctx context.Context, kubeClient kubernetes.Interface) error {
    logger := klog.FromContext(ctx)

    // List all ResourceClaims across all namespaces
    claims, err := kubeClient.ResourceV1().ResourceClaims("").List(ctx, metav1.ListOptions{})
    if err != nil {
        return fmt.Errorf("list resource claims: %w", err)
    }

    for i := range claims.Items {
        claim := &claims.Items[i]

        // Skip unallocated claims
        if claim.Status.Allocation == nil {
            continue
        }

        // Skip if no results for our driver
        hasOurDriver := false
        for _, result := range claim.Status.Allocation.Devices.Results {
            if result.Driver == ex.driverName {
                hasOurDriver = true
                break
            }
        }
        if !hasOurDriver {
            continue
        }

        if err := ex.reconcileClaimBindingConditions(ctx, claim, kubeClient); err != nil {
            logger.Error(err, "Failed to reconcile claim", "claim", klog.KObj(claim))
        }
    }
    return nil
}

// reconcileClaimBindingConditions updates device status conditions for a single claim.
func (ex *ExamplePlugin) reconcileClaimBindingConditions(ctx context.Context, claim *resourceapi.ResourceClaim, kubeClient kubernetes.Interface) error {
    logger := klog.FromContext(ctx)

    updatedClaim := claim.DeepCopy()
    needsUpdate := false

    for _, deviceResult := range claim.Status.Allocation.Devices.Results {
        if deviceResult.Driver != ex.driverName {
            continue
        }
        if len(deviceResult.BindingConditions) == 0 {
            continue
        }

        scenario := ex.getDeviceScenario(deviceResult.Device)
        logger.V(5).Info("Reconciling binding conditions",
            "claim", klog.KObj(claim),
            "device", deviceResult.Device,
            "scenario", scenario,
        )

        deviceStatus := findOrCreateDeviceStatus(updatedClaim, deviceResult)

        switch scenario {
        case "normal":
            needsUpdate = setBindingCondition(deviceStatus,
                "FabricDeviceReady",
                metav1.ConditionTrue,
                "DeviceReady",
                "Device is ready for binding",
            ) || needsUpdate

        case "retry":
            if claim.Status.Allocation.AllocationTimestamp != nil &&
                time.Since(claim.Status.Allocation.AllocationTimestamp.Time) >= 10*time.Second {
                needsUpdate = setBindingCondition(deviceStatus,
                    "FabricDeviceReady",
                    metav1.ConditionTrue,
                    "DeviceReady",
                    "Device is ready after retry delay",
                ) || needsUpdate
            } else {
                logger.V(5).Info("Retry scenario: not ready yet", "device", deviceResult.Device)
            }

        case "avoid":
            needsUpdate = setBindingCondition(deviceStatus,
                "FabricDeviceReschedule",
                metav1.ConditionTrue,
                "RescheduleRequired",
                "Device requires rescheduling to another node",
            ) || needsUpdate

        case "reconsider":
            needsUpdate = setBindingCondition(deviceStatus,
                "FabricDeviceFailed",
                metav1.ConditionTrue,
                "BindingFailed",
                "Device binding has permanently failed",
            ) || needsUpdate

        case "timeout":
            // Intentionally do nothing
            logger.V(5).Info("Timeout scenario: not setting conditions", "device", deviceResult.Device)
        }
    }

    if !needsUpdate {
        return nil
    }

    logger.V(4).Info("Updating claim binding conditions", "claim", klog.KObj(updatedClaim))
    _, err := kubeClient.ResourceV1().ResourceClaims(updatedClaim.Namespace).
        UpdateStatus(ctx, updatedClaim, metav1.UpdateOptions{})
    return err
}

// updateBindingConditions inspects each allocated device in the claim,
// looks up its scenario attribute, and sets the appropriate conditions
// on claim.Status.Devices.
func (ex *ExamplePlugin) updateBindingConditions(ctx context.Context, claim *resourceapi.ResourceClaim) error {
    logger := klog.FromContext(ctx)

    if claim.Status.Allocation == nil {
        return nil
    }

    updatedClaim := claim.DeepCopy()
    needsUpdate := false

    for _, deviceResult := range claim.Status.Allocation.Devices.Results {
        // Only handle our driver's devices
        if deviceResult.Driver != ex.driverName {
            continue
        }
        // Skip devices with no binding conditions
        if len(deviceResult.BindingConditions) == 0 {
            continue
        }

        scenario := ex.getDeviceScenario(deviceResult.Device)
        logger.V(5).Info("Setting binding conditions",
            "claim", klog.KObj(claim),
            "device", deviceResult.Device,
            "scenario", scenario,
        )

        deviceStatus := findOrCreateDeviceStatus(updatedClaim, deviceResult)

        switch scenario {
        case "normal":
            // Immediately ready → success path in scheduler
            needsUpdate = setBindingCondition(deviceStatus, "FabricDeviceReady",
                metav1.ConditionTrue, "DeviceReady", "Device is ready for binding") || needsUpdate

        case "retry":
            // Simulate delay: set ready after 10 seconds
            if claim.Status.Allocation.AllocationTimestamp != nil &&
                time.Since(claim.Status.Allocation.AllocationTimestamp.Time) >= 10*time.Second {
                needsUpdate = setBindingCondition(deviceStatus, "FabricDeviceReady",
                    metav1.ConditionTrue, "DeviceReady", "Device is ready after retry") || needsUpdate
            } else {
                logger.V(5).Info("Retry scenario: not ready yet, waiting", "device", deviceResult.Device)
            }

        case "avoid":
            // Set failure condition → triggers ErrDeviceBindingFailed in scheduler
            needsUpdate = setBindingCondition(deviceStatus, "FabricDeviceReschedule",
                metav1.ConditionTrue, "RescheduleRequired", "Device requires rescheduling to another node") || needsUpdate

        case "reconsider":
            // Set failure condition → triggers ErrDeviceBindingFailed in scheduler
            needsUpdate = setBindingCondition(deviceStatus, "FabricDeviceFailed",
                metav1.ConditionTrue, "BindingFailed", "Device binding has permanently failed") || needsUpdate

        case "timeout":
            // Intentionally set nothing → scheduler poll will hit bindingTimeout
            logger.V(5).Info("Timeout scenario: not setting any conditions", "device", deviceResult.Device)

        default:
            logger.V(5).Info("Unknown scenario, skipping", "device", deviceResult.Device, "scenario", scenario)
        }
    }

    if !needsUpdate {
        return nil
    }

    logger.V(4).Info("Updating claim binding conditions status", "claim", klog.KObj(updatedClaim))
    _, err := ex.UpdateStatus(ctx, updatedClaim)
    return err
}

// getDeviceScenario looks up the scenario attribute for a device by name
// from the driver's published resource slices.
func (ex *ExamplePlugin) getDeviceScenario(deviceName string) string {
    if ex.fileOps.DriverResources == nil {
        return "normal"
    }

    scenarioAttrName := resourceapi.QualifiedName(fmt.Sprintf("%s/scenario", ex.driverName))

    for _, pool := range ex.fileOps.DriverResources.Pools {
        for _, slice := range pool.Slices {
            for _, device := range slice.Devices {
                if device.Name == deviceName {
                    if attr, ok := device.Attributes[scenarioAttrName]; ok && attr.StringValue != nil {
                        return *attr.StringValue
                    }
                }
            }
        }
    }
    return "normal" // safe default
}

// findOrCreateDeviceStatus finds an existing AllocatedDeviceStatus entry
// or appends a new one and returns a pointer to it.
func findOrCreateDeviceStatus(
    claim *resourceapi.ResourceClaim,
    deviceResult resourceapi.DeviceRequestAllocationResult,
) *resourceapi.AllocatedDeviceStatus {
    for i := range claim.Status.Devices {
        d := &claim.Status.Devices[i]
        if d.Driver == deviceResult.Driver &&
            d.Pool == deviceResult.Pool &&
            d.Device == deviceResult.Device {
            return d
        }
    }
    claim.Status.Devices = append(claim.Status.Devices, resourceapi.AllocatedDeviceStatus{
        Driver: deviceResult.Driver,
        Pool:   deviceResult.Pool,
        Device: deviceResult.Device,
    })
    return &claim.Status.Devices[len(claim.Status.Devices)-1]
}

// setBindingCondition sets or updates a condition on a device status entry.
// Returns true if the condition was changed (i.e. an API update is needed).
func setBindingCondition(
    deviceStatus *resourceapi.AllocatedDeviceStatus,
    condType string,
    status metav1.ConditionStatus,
    reason, message string,
) bool {
    now := metav1.Now()
    for i, cond := range deviceStatus.Conditions {
        if cond.Type == condType {
            if cond.Status == status {
                return false // already correct, no update needed
            }
            deviceStatus.Conditions[i].Status = status
            deviceStatus.Conditions[i].Reason = reason
            deviceStatus.Conditions[i].Message = message
            deviceStatus.Conditions[i].LastTransitionTime = now
            return true
        }
    }
    // Condition doesn't exist yet, append it
    deviceStatus.Conditions = append(deviceStatus.Conditions, metav1.Condition{
        Type:               condType,
        Status:             status,
        Reason:             reason,
        Message:            message,
        LastTransitionTime: now,
    })
    return true
}
