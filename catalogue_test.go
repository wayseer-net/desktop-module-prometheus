package prometheus

import (
	"testing"

	"wayseer.dev/sdk"
)

func TestUnitsComeFromMetricNames(t *testing.T) {
	for _, c := range []struct {
		name, unit string
		want       sdk.Unit
	}{
		{"node_memory_MemTotal_bytes", "", sdk.UnitBytes},
		{"node_hwmon_temp_celsius", "", sdk.UnitCelsius},
		{"node_cpu_scaling_frequency_hertz", "", sdk.UnitHertz},
		{"node_hwmon_in_volts", "", sdk.UnitVolts},
		{"node_hwmon_curr_amps", "", sdk.UnitAmperes},
		{"node_hwmon_power_average_watt", "", sdk.UnitWatts},
		{"ups_load_watts", "", sdk.UnitWatts},
		{"weather_pressure", "pascals", sdk.UnitPascals},
		{"room_light_lux", "", sdk.UnitLux},
		{"queue_depth", "", sdk.UnitNone},
	} {
		if got := gaugeUnit(c.name, c.unit); got != c.want {
			t.Errorf("gaugeUnit(%q, %q) = %q, want %q", c.name, c.unit, got, c.want)
		}
	}
	for name, want := range map[string]sdk.Unit{
		"node_rapl_package_joules_total":   sdk.UnitWatts, // joules per second
		"node_network_receive_bytes_total": sdk.UnitBytesPS,
		"http_requests_total":              sdk.UnitPerSec,
	} {
		if got := rateUnit(name); got != want {
			t.Errorf("rateUnit(%q) = %q, want %q", name, got, want)
		}
	}
}
