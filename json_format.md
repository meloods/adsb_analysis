# 📄 JSON File Format: `trace_full_<ICAO-hex-id>.json`

Each file represents trace data for a specific aircraft on a given day. These files are named using the ICAO hex identifier of the aircraft:

```
trace_full_<ICAO-hex-id>.json
```

## 🧩 Top-Level Keys

### 🔒 Required Keys

| Key         | Type   | Description                                                            |
| ----------- | ------ | ---------------------------------------------------------------------- |
| `icao`      | string | The 6-digit ICAO hex identifier of the aircraft (e.g., `"a1b2c3"`).    |
| `timestamp` | float  | The base UNIX timestamp (seconds since epoch) for the trace data.      |
| `trace`     | array  | An array of arrays representing timestamped state vectors (see below). |

### ❓ Optional Keys

| Key       | Type   | Description                                                           |
| --------- | ------ | --------------------------------------------------------------------- |
| `r`       | string | Aircraft registration (pulled from database).                         |
| `t`       | string | Aircraft type (pulled from database).                                 |
| `desc`    | string | Long type description, if available via `--db-file-lt`.               |
| `dbFlags` | int    | Bitfield encoding database metadata flags. Use bitwise AND to decode: |

#### 🔢 `dbFlags` Definitions

| Bit Position | Decimal Value | Meaning                               |
| ------------ | ------------- | ------------------------------------- |
| Bit 0        | `1`           | `dbFlags & 1`: Military aircraft      |
| Bit 1        | `2`           | `dbFlags & 2`: Interesting aircraft   |
| Bit 2        | `4`           | `dbFlags & 4`: PIA aircraft           |
| Bit 3        | `8`           | `dbFlags & 8`: LADD-obscured aircraft |

## ✈️ `trace` Array Format

Each element in the `trace` array is a 14-element array representing a single point in the aircraft's movement trace.

| Index | Field                         | Type                    | Description                                            |
| ----- | ----------------------------- | ----------------------- | ------------------------------------------------------ |
| 0     | `seconds_after_timestamp`     | float                   | Time offset from `timestamp` (in seconds).             |
| 1     | `latitude`                    | float                   | Latitude in decimal degrees.                           |
| 2     | `longitude`                   | float                   | Longitude in decimal degrees.                          |
| 3     | `altitude_ft`                 | int \| "ground" \| null | Barometric altitude in feet.                           |
| 4     | `ground_speed_kts`            | float \| null           | Ground speed in knots.                                 |
| 5     | `track_deg`                   | float \| null           | Track angle (degrees); if on ground, this is heading.  |
| 6     | `flags_bitfield`              | int                     | Bitfield of flight-related flags (see below).          |
| 7     | `vertical_rate_fpm`           | int \| null             | Vertical rate in feet per minute.                      |
| 8     | `aircraft_metadata`           | dict \| null            | Aircraft state snapshot (see details below).           |
| 9     | `source_type`                 | str \| null             | Source type (e.g., `"adsb_icao"`, `"tisb_trackfile"`). |
| 10    | `geometric_altitude_ft`       | int \| null             | GNSS/INS-derived altitude (feet).                      |
| 11    | `geometric_vertical_rate_fpm` | int \| null             | Geometric vertical speed (ft/min).                     |
| 12    | `indicated_airspeed_kts`      | float \| null           | Indicated airspeed (IAS) in knots.                     |
| 13    | `roll_angle_deg`              | float \| null           | Aircraft roll angle in degrees.                        |

## 🚩 `flags_bitfield` – Bitwise Flags

The `flags_bitfield` encodes multiple boolean flight conditions using a single integer. Use bitwise operations to decode.

| Bit Position | Decimal Value | Description                                                  |
| ------------ | ------------- | ------------------------------------------------------------ |
| Bit 0        | `1`           | Position is **stale** (no update in 20s).                    |
| Bit 1        | `2`           | Start of a **new leg** (likely takeoff or landing boundary). |
| Bit 2        | `4`           | Vertical rate is **geometric** (GNSS-derived).               |
| Bit 3        | `8`           | Altitude is **geometric** (GNSS-derived), not barometric.    |

#### 🧠 Example

```python
flags = 5  # binary 0101

if flags & 1:
    print("Position is stale")
if flags & 2:
    print("Start of a new leg")
if flags & 4:
    print("Vertical rate is geometric")
if flags & 8:
    print("Altitude is geometric")
```

## 🧬 `aircraft_metadata` (Optional Dictionary)

Not all keys are guaranteed to be present.

### 🔍 Identification and Source

- `hex`: ICAO hex identifier (may start with `~` for non-ICAO sources).
- `type`: Best available message source type. Possible values:
  - `adsb_icao`, `adsb_icao_nt`, `adsr_icao`, `tisb_icao`, `adsc`, `mlat`
  - `other`, `mode_s`, `adsb_other`, `adsr_other`, `tisb_other`, `tisb_trackfile`

### 📡 Flight and Positioning

- `flight`: Callsign or registration.
- `alt_baro`: Barometric altitude (`int` or `"ground"`).
- `alt_geom`: Geometric altitude (feet).
- `lat`, `lon`: Position (decimal degrees).
- `gs`, `ias`, `tas`: Ground speed, indicated airspeed, true airspeed (knots).
- `mach`: Mach number.
- `track`, `track_rate`: Track angle and rate of change.
- `roll`: Roll angle in degrees.
- `mag_heading`, `true_heading`: Heading values.
- `baro_rate`, `geom_rate`: Climb/descent rates.

### ⚙️ Transponder & Status

- `squawk`: 4-digit Mode A code (octal).
- `emergency`: Emergency status: `none`, `general`, `lifeguard`, etc.
- `category`: Emitter category (`A0`–`D7`).
- `alert`: Alert bit (DO-260B).
- `spi`: Special Position Identification.

### 🧭 Navigation Settings

- `nav_qnh`: Altimeter setting (hPa).
- `nav_altitude_mcp`, `nav_altitude_fms`: Selected altitudes.
- `nav_heading`: Selected heading.
- `nav_modes`: Autopilot mode(s) (e.g., `vnav`, `lnav`, `althold`).

### 📊 Integrity & Accuracy

- `nic`, `nic_baro`, `nac_p`, `nac_v`: Integrity and accuracy codes.
- `version`: ADS-B version.
- `sil`, `sil_type`: Source Integrity Level and type.
- `gva`, `sda`: Vertical accuracy and design assurance.
- `rc`: Radius of Containment.

### 🛰️ Surveillance

- `mlat`: Fields present due to multilateration.
- `tisb`: Fields derived from TIS-B.

### 🔁 Message Timing

- `messages`: Total messages received.
- `seen`, `seen_pos`: Seconds since last message or position update.
- `rssi`: Signal strength (dBFS).

### 🌬️ Derived Environmental Data

- `wd`, `ws`: Wind direction/speed.
- `oat`, `tat`: Outside/Total air temperature (°C).  
  *Note: Values may be less accurate below Mach 0.5.*

## 📌 Notes

- This format is optimized for efficient ingestion and trace-level filtering.
- Optional fields should be handled with null checks in downstream processing.
- Bitfields (`flags_bitfield`, `dbFlags`) are performance-optimized encodings and must be interpreted with care.

## Fields of Interest

Here are the trace (and supporting-metadata) fields you’ll want to pull out for the study, grouped by why they matter:

| Why you need it                                     | Fields to extract                                                                                              | What you’ll use them for                                                                                                                                                                          |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Identify each flight**                            | `icao` (hex address) ± callsign (`aircraft_metadata.flight`)                                                   | Keep counts per aircraft and avoid double-counting when it re-enters a zone.                                                                                                                      |
| **Match the two daily windows**                     | `timestamp` + `seconds_after_timestamp` → absolute UTC time (then shift +8 h to SGT)                           | Select only points that fall inside 09:00-12:00 SGT and 19:00-21:00 SGT.                                                                                                                          |
| **Locate the aircraft**                             | `latitude`, `longitude`                                                                                        | Test whether a point lies inside Changi / Seletar airport polygons, Areas A–H, the red or grey zones, or the designated E/W routes.                                                               |
| **Altitude-based filtering & reporting**            | `altitude_ft` (primary barometric) and, when present, `geometric_altitude_ft`                                  | Report requested altitudes for over-flights and filter out ground movements when you only want airborne traffic.                                                                                  |
| **Determine heading / cardinal direction**          | `track_deg` (or `aircraft_metadata.true_heading` as backup)                                                    | Classify each over-flight as north-, south-, east- or west-bound; verify south-bound departures / arrivals at Seletar; decide whether an aircraft is using the east- or west-bound route.         |
| **Spot departures vs. arrivals**                    | `flags_bitfield` (Bit 1 = “new leg”), `ground_speed_kts`, `vertical_rate_fpm`, `altitude_ft` ≈ 0 or `"ground"` | Detect the take-off and landing segment that brackets a leg, distinguish departures from arrivals at Changi, and confirm that Seletar movements are indeed south-bound or inbound from the south. |
| **Trajectory visualisation (red zone requirement)** | All of the above (lat, lon, time, altitude, track) pulled in sequence                                          | Plot full paths across the red-shaded zone for the two special dates.                                                                                                                             |

In practice you’ll usually read only indices 0–7 and 10 of each trace entry, plus the top-level `icao` and `timestamp`. The richer fields inside `aircraft_metadata` are optional backups if the basic ones are null.