from es_sfgtools.novatel_tools import extract_rangea_from_qcpin

# Test data with 4 RANGEA entries (but 007BE1 and 007BEB have same GPS time, so only 3 unique)
data = {
    "interrogation": {
        "observations": {
            "NOV_RANGE": {
                "raw": "#RANGEA,USB2,0,73.5,FINESTEERING,2379,414835.000,02000000,5103,17136;29,10,0,21226572.227,0.088,-111546445.541361,0.011,-1044.146,51.5,15479.375,1810dc04,10,0,21226580.909,0.027,-86919367.542363,0.013,-813.621,51.9,15479.375,11305c0b*85f7e08f"
            }
        }
    },
    "007BE1": {
        "observations": {
            "NOV_RANGE": {
                "raw": "#RANGEA,USB2,0,73.0,FINESTEERING,2379,414837.400,02000000,5103,17136;29,10,0,21227047.960,0.099,-111548945.507366,0.009,-1041.010,50.4,15481.775,1810dc04,10,0,21227056.640,0.036,-86921315.562892,0.012,-811.178,49.2,15481.775,11305c0b*f00dc51e"
            }
        }
    },
    "007BEB": {
        "observations": {
            "NOV_RANGE": {
                # Same GPS time as 007BE1 - should be deduplicated
                "raw": "#RANGEA,USB2,0,73.0,FINESTEERING,2379,414837.400,02000000,5103,17136;29,10,0,21227047.960,0.099,-111548945.507366,0.009,-1041.010,50.4,15481.775,1810dc04,10,0,21227056.640,0.036,-86921315.562892,0.012,-811.178,49.2,15481.775,11305c0b*f00dc51e"
            }
        }
    },
    "007BEC": {
        "observations": {
            "NOV_RANGE": {
                "raw": "#RANGEA,USB2,0,73.0,FINESTEERING,2379,414837.600,02000000,5103,17136;29,10,0,21227087.610,0.100,-111549153.861782,0.010,-1042.124,50.3,15481.976,1810dc04,10,0,21227096.289,0.034,-86921477.916948,0.012,-812.046,49.8,15481.976,11305c0b*2260ce45"
            }
        }
    }
}

epochs = extract_rangea_from_qcpin(data)
print(f"Found {len(epochs)} unique epochs (expected 3 - one duplicate)")

for e in epochs:
    print(f"  GPS week {e.gps_week}, seconds {e.gps_seconds}: {e.satellite_count} satellites, {e.num_observations} obs")

# Verify deduplication
assert len(epochs) == 3, f"Expected 3 unique epochs, got {len(epochs)}"

# Verify GPS times
gps_times = [(e.gps_week, e.gps_seconds) for e in epochs]
assert (2379, 414835.0) in gps_times
assert (2379, 414837.4) in gps_times
assert (2379, 414837.6) in gps_times

print("\nAll tests passed!")
