package app

import (
	"encoding/json"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Binding failure policies – kept in sync with scheduler plugin.
const (
	bindingPolicyReconsider = "DRAFailurePolicyReconsidered"
	bindingPolicyRetry      = "DRAFailurePolicyRetried"
	bindingPolicyAvoid      = "DRAFailurePolicyAvoided"
)

// getScenarioFromClaim inspects the claim config for our test "scenario" parameter.
func getScenarioFromClaim(claim *resourceapi.ResourceClaim) string {
	if claim == nil {
		return ""
	}

	// We read the scenario from spec.devices.config[*].opaque.parameters["scenario"].
	for _, cfg := range claim.Spec.Devices.Config {
		if cfg.Opaque == nil {
			continue
		}
		if cfg.Opaque.Driver != "test-driver.cdi.k8s.io" {
			continue
		}
		if len(cfg.Opaque.Parameters.Raw) == 0 {
			continue
		}

		var params map[string]string
		if err := json.Unmarshal(cfg.Opaque.Parameters.Raw, &params); err != nil {
			// Not a simple string map → ignore.
			continue
		}
		if s, ok := params["scenario"]; ok {
			return s
		}
	}
	return ""
}

// upsertDeviceStatus finds (or creates) an AllocatedDeviceStatus entry
// for the given driver/pool/device triple.
func upsertDeviceStatus(
	claim *resourceapi.ResourceClaim,
	driver, pool, device string,
) *resourceapi.AllocatedDeviceStatus {
	if claim.Status.Devices == nil {
		claim.Status.Devices = []resourceapi.AllocatedDeviceStatus{}
	}

	for i := range claim.Status.Devices {
		ds := &claim.Status.Devices[i]
		if ds.Driver == driver && ds.Pool == pool && ds.Device == device {
			return ds
		}
	}

	ds := resourceapi.AllocatedDeviceStatus{
		Driver: driver,
		Pool:   pool,
		Device: device,
	}
	claim.Status.Devices = append(claim.Status.Devices, ds)
	return &claim.Status.Devices[len(claim.Status.Devices)-1]
}

// setDeviceCondition appends or updates a single condition on that device.
func setDeviceCondition(
	ds *resourceapi.AllocatedDeviceStatus,
	condType, reason, message string,
	status metav1.ConditionStatus,
) {
	if ds.Conditions == nil {
		ds.Conditions = []metav1.Condition{}
	}

	// Simple replace-if-exists semantics by Type.
	for i := range ds.Conditions {
		if ds.Conditions[i].Type == condType {
			ds.Conditions[i].Status = status
			ds.Conditions[i].Reason = reason
			ds.Conditions[i].Message = message
			ds.Conditions[i].LastTransitionTime = metav1.Now()
			return
		}
	}

	ds.Conditions = append(ds.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	})
}

// applyBindingScenario sets AllocatedDeviceStatus.Conditions based on the test scenario.
//
// It assumes claim.Status.Allocation is already populated (i.e. allocation
// controller has picked devices).
func applyBindingScenario(claim *resourceapi.ResourceClaim) {
	if claim == nil || claim.Status.Allocation == nil {
		return
	}

	scenario := getScenarioFromClaim(claim)

	for _, res := range claim.Status.Allocation.Devices.Results {
		ds := upsertDeviceStatus(claim, res.Driver, res.Pool, res.Device)

		switch scenario {
		case "", "normal", "report":
			// Happy path: device becomes ready, no failure conditions.
			// BindingConditions: FabricDeviceReady == True
			setDeviceCondition(
				ds,
				"FabricDeviceReady",
				"Bound",
				"device bound successfully",
				metav1.ConditionTrue,
			)

		case "timeout":
			// We deliberately do *nothing*.
			// Scheduler sees BindingConditions but no matching Condition == True,
			// so it will hit its binding timeout.

		case "retry":
			// Retry policy: use one of the BindingFailureConditions, e.g. FabricDeviceReschedule.
			setDeviceCondition(
				ds,
				"FabricDeviceReschedule",
				bindingPolicyRetry,
				"driver requests re-scheduling of the same device",
				metav1.ConditionTrue,
			)

		case "avoid":
			// Avoid policy: mark device as failed and let scheduler de-prioritize it.
			setDeviceCondition(
				ds,
				"FabricDeviceFailed",
				bindingPolicyAvoid,
				"driver requests avoiding this device",
				metav1.ConditionTrue,
			)

		case "reconsider":
			// Reconsider = we tell scheduler that binding failed, but we *don't* use special
			// retry/avoid handling in PreBind. It will go through the default error path.
			setDeviceCondition(
				ds,
				"FabricDeviceFailed",
				bindingPolicyReconsider,
				"driver reports binding failure; scheduler should reconsider this pod",
				metav1.ConditionTrue,
			)
		}
	}
}
