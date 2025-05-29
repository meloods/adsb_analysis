# Aircraft Movement Analysis using Data from [adsb.lol](https://github.com/adsblol)

---

## 📚 Table of Contents

- [How to Download and Extract Data](#-how-to-download-and-extract-data)
- [Understanding the Extracted Files](#-understanding-the-extracted-files)
  - [`trace_full_<ICAO-hex-id>.json`](#trace_full_icao-hex-idjson)
  - [`aircraft.json`](#️aircraftjson)
- [Dates of interest](#dates-of-interest)
- [Python Environment Management (Anaconda + WSL2)](#-python-environment-management-anaconda--wsl2)
- [Others](#others)

---

## 📥 How to Download and Extract Data

Data is available via the **Releases** tab on the following repositories:

* [globe\_history\_2025](https://github.com/adsblol/globe_history_2025)
* [globe\_history\_2024](https://github.com/adsblol/globe_history_2024)

Each release may consist of one or more `.tar` split files (e.g., `.aa`, `.ab`, etc.). To extract:

```bash
# Create a directory for the specific date
mkdir -p 2025.05.18

# Concatenate and extract split tar files
cat v2025.05.18-planes-readsb-prod-0.tar.aa v2025.05.18-planes-readsb-prod-0.tar.ab | tar -xf - -C 2025.05.18
```

Adjust the filenames and folder names according to the date and files you've downloaded.

### OR 

run `python scripts/get_traces.py YYYY.MM.DD`

The data will be stored in the following format:
```
data/
└── 2025.05.18/
    ├── downloaded/
    ├── extracted/
    │   └── traces/
    │       ├── 00/
    │       ├── 01/
    │       └── ...
    └── traces/
        ├── abc123.json
        ├── def456.json
        └── ...
```
the `.json` files in the `traces` folder 

---

## 📂 Understanding the Extracted Files

### `trace_full_<ICAO-hex-id>.json`

Each file contains time-stamped position reports for a specific aircraft over a single day.
> Note: While files are named .json, they are actually gzip-compressed. Use gzip.open() or CLI gunzip -c to read them.

| Key         | Type   | Description                                                                      |
| ----------- | ------ | -------------------------------------------------------------------------------- |
| `icao`      | string | ICAO hex identifier (e.g., `"0123ac"`). May start with `~` for non-ICAO sources. |
| `timestamp` | int    | Base UNIX timestamp (seconds since epoch).                                       |
| `trace`     | array  | Array of aircraft state vectors (see format below).                              |

#### 📊 Trace Array Format (Field-by-Field)

| Index | Field                         | Type                    | Description                                               |
| ----- | ----------------------------- | ----------------------- | --------------------------------------------------------- |
| 0     | `seconds_after_timestamp`     | float                   | Time offset from base timestamp (in seconds)              |
| 1     | `latitude`                    | float                   | Latitude in decimal degrees                               |
| 2     | `longitude`                   | float                   | Longitude in decimal degrees                              |
| 3     | `altitude_ft`                 | int \| "ground" \| null | Barometric altitude in feet                               |
| 4     | `ground_speed_kts`            | float \| null           | Ground speed in knots                                     |
| 5     | `track_deg`                   | float \| null           | Track angle (degrees); if on ground, this is true heading |
| 6     | `flags_bitfield`              | int                     | Bitfield flags (see [Flags Bitfield](#flags-bitfield))    |
| 7     | `vertical_rate_fpm`           | int \| null             | Vertical speed in feet per minute                         |
| 8     | `aircraft_metadata`           | dict \| null            | Additional info (see [`aircraft.json`](#aircraftjson))    |
| 9     | `source_type`                 | str \| null             | Source (e.g., `"adsb_icao"`, `"tisb_trackfile"`)          |
| 10    | `geometric_altitude_ft`       | int \| null             | GNSS/INS geometric altitude (ft)                          |
| 11    | `geometric_vertical_rate_fpm` | int \| null             | Geometric vertical rate (ft/min)                          |
| 12    | `indicated_airspeed_kts`      | float \| null           | IAS in knots                                              |
| 13    | `roll_angle_deg`              | float \| null           | Aircraft roll angle in degrees                            |


#### 🏷️ Flags Bitfield

The `flags` field in each `trace` entry is an **integer bitfield**, meaning it encodes multiple boolean flags within a single number. Each bit represents a specific flight-related condition.


##### 📊 Flag Definitions

To decode the flag, use **bitwise AND (`&`)** operations:

| Bit Position | Decimal Value | Meaning                                                                               |
| ------------ | ------------- | ------------------------------------------------------------------------------------- |
| Bit 0        | `1`           | `flags & 1 > 0`: Position is **stale** (no new position received in 20s)              |
| Bit 1        | `2`           | `flags & 2 > 0`: Start of a **new leg** (likely boundary between takeoff and landing) |
| Bit 2        | `4`           | `flags & 4 > 0`: **Vertical rate** is **geometric** (GNSS-derived), not barometric    |
| Bit 3        | `8`           | `flags & 8 > 0`: **Altitude** is **geometric**, not barometric                        |

##### 🧠 What This Means

* A value like `flags = 5` means bits 0 and 2 are set (because `5 = 0b0101`):

  * ✅ Position is stale
  * ✅ Vertical rate is geometric
  * ❌ Not a new leg
  * ❌ Altitude is barometric

This allows you to efficiently check multiple conditions for a trace entry using just one integer field.

##### 🛠️ Python Example

```python
flags = 5  # Example value

if flags & 1:
    print("Position is stale")
if flags & 2:
    print("Start of a new leg")
if flags & 4:
    print("Vertical rate is geometric")
if flags & 8:
    print("Altitude is geometric")
```

#### Example of a `trace_full_<ICAO-hex-id>.json` file

```json
{
    "icao":"3c66b0",
    "r":"D-AIUP",
    "t":"A320",
    "dbFlags":0,
    "desc":"AIRBUS A-320",
    "timestamp": 1663259853.016,
    "trace":[
        [7016.59,49.263300,10.614239,25125,446.5,309.0,0,-2176,
            {"type":"adsb_icao","flight":"DLH7YA  ","alt_geom":25875,"ias":335,"tas":484,"mach":0.796,"wd":297,"ws":40,"oat":-30,"tat":1,"track":309.00,"track_rate":-0.53,"roll":-10.72,"mag_heading":304.28,"true_heading":308.02,"baro_rate":-2176,"geom_rate":-2208,"squawk":"1000","category":"A3","nav_qnh":1012.8,"nav_altitude_mcp":14016,"nic":8,"rc":186,"version":2,"nic_baro":1,"nac_p":8,"nac_v":0,"sil":3,"sil_type":"perhour","gva":2,"sda":2,"alert":0,"spi":0},
            "adsb_icao",25875,-2208,335,-10.7],
        [7024.85,49.273589,10.593278,24825,446.0,306.6,0,-2176,null,"adsb_icao",25550,-2144,337,-1.6],
        [7035.67,49.286865,10.565890,24425,446.8,306.5,0,-2176,null,"adsb_icao",25150,-2144,339,0.3],
        [7046.71,49.300403,10.537985,24025,446.8,306.5,0,-2176,null,"adsb_icao",24775,-2176,341,0.3],
        [7057.80,49.314042,10.509941,23625,445.2,306.7,0,-2176,
            {"type":"adsb_icao","flight":"DLH7YA  ","alt_geom":24325,"ias":339,"tas":482,"mach":0.784,"wd":296,"ws":37,"oat":-24,"tat":6,"track":306.69,"track_rate":0.00,"roll":0.18,"mag_heading":302.17,"true_heading":305.89,"baro_rate":-2176,"geom_rate":-2176,"squawk":"1000","category":"A3","nav_qnh":1012.8,"nav_altitude_mcp":14016,"nic":8,"rc":186,"version":2,"nic_baro":1,"nac_p":8,"nac_v":0,"sil":3,"sil_type":"perhour","gva":2,"sda":2,"alert":0,"spi":0},
            "adsb_icao",24325,-2176,339,0.2],
        [7068.82,49.327469,10.482225,23250,443.2,306.6,0,-2112,null,"adsb_icao",23925,-2144,340,0.2],
        [7080.53,49.341694,10.452841,22875,441.2,306.4,0,-1728,null,"adsb_icao",23550,-1728,341,-0.2]
}
```

---

### 🛩️`aircraft.json`

Note that keys will be omitted if data is not available.

#### 🔍 Identification and Source

* **`hex`**: The 24-bit ICAO identifier of the aircraft, represented as 6 hexadecimal digits. If the identifier starts with `~`, it is a **non-ICAO address** (e.g., from TIS-B).
* **`type`**: The **underlying message type** or **best available data source** for this aircraft. Listed in order of data reliability:

  * `adsb_icao`: Mode S / ADS-B transponder using 24-bit ICAO address
  * `adsb_icao_nt`: Non-transponder ADS-B emitter (e.g., ground vehicle) with 24-bit ICAO
  * `adsr_icao`: Rebroadcast ADS-B via another link (e.g., UAT), with ICAO address
  * `tisb_icao`: Secondary radar data, 24-bit ICAO address
  * `adsc`: ADS-C (via satellite downlink)
  * `mlat`: Multilateration using arrival-time differences; accuracy may vary
  * `other`: Miscellaneous Basestation/SBS-formatted data
  * `mode_s`: Mode S data (no position included)
  * `adsb_other`: ADS-B with non-ICAO address (e.g., anonymized)
  * `adsr_other`: Rebroadcast ADS-B with non-ICAO address
  * `tisb_other`: TIS-B target with non-ICAO address
  * `tisb_trackfile`: Non-ADS-B target identified by radar-based track/file ID

#### 📡 Flight and Positioning Info

* **`flight`**: Callsign or aircraft registration (max 8 characters)
* **`alt_baro`**: Barometric altitude (ft) or `"ground"` (string)
* **`alt_geom`**: Geometric (GNSS/INS) altitude in feet (WGS84 reference)
* **`lat`, `lon`**: Decimal degree position
* **`gs`**: Ground speed (knots)
* **`ias`**: Indicated airspeed (knots)
* **`tas`**: True airspeed (knots)
* **`mach`**: Mach number
* **`track`**: True track over ground (0–359°)
* **`track_rate`**: Track rate of change (°/second)
* **`roll`**: Aircraft roll angle in degrees (negative is left roll)
* **`mag_heading`**: Heading from magnetic north (°)
* **`true_heading`**: Heading from true north (°); often derived in air
* **`baro_rate`**: Climb/descent rate from barometric altitude (ft/min)
* **`geom_rate`**: Climb/descent rate from geometric altitude (ft/min)

#### ⚙️ Transponder and Status Info

* **`squawk`**: 4-digit Mode A code (octal)
* **`emergency`**: ADS-B emergency code
  Options: `none`, `general`, `lifeguard`, `minfuel`, `nordo`, `unlawful`, `downed`, `reserved`
* **`category`**: Emitter classification (`A0`–`D7`)
* **`alert`**: Alert status bit (see DO-260B §2.2.3.2.3.2)
* **`spi`**: Special Position Identification bit

#### 🧭 Navigation Settings

* **`nav_qnh`**: Altimeter setting (hPa)
* **`nav_altitude_mcp`**: Selected altitude from MCP/FCU
* **`nav_altitude_fms`**: Selected altitude from FMS
* **`nav_heading`**: Selected heading (typically magnetic)
* **`nav_modes`**: Active autopilot modes (e.g., `'autopilot', 'vnav', 'althold', 'approach', 'lnav', 'tcas'`)

#### 📊 Accuracy & Integrity Metrics

* **`nic`**: Navigation Integrity Category
* **`rc`**: Radius of Containment (m)
* **`version`**: ADS-B Version (0, 1, 2; 3–7 reserved)
* **`nic_baro`**: Barometric NIC
* **`nac_p`**: NAC for position
* **`nac_v`**: NAC for velocity
* **`sil`**: Source Integrity Level
* **`sil_type`**: SIL interpretation (`unknown`, `perhour`, `persample`)
* **`gva`**: Geometric Vertical Accuracy
* **`sda`**: System Design Assurance

#### 🛰️ Surveillance Info

* **`mlat`**: List of fields from MLAT-derived positions
* **`tisb`**: List of fields from TIS-B-derived positions

#### 🔁 Timeliness and Message Stats

* **`messages`**: Total Mode S messages received
* **`seen`**: Seconds since last message
* **`seen_pos`**: Seconds since last position update
* **`rssi`**: Average signal power (dBFS, always negative)

#### 🌬️ Environmental Parameters (Calculated)

* **`wd`**, **`ws`**: Wind direction and speed (derived)
* **`oat`**, **`tat`**: Outside air temperature & total air temperature (°C)

  > *Note*: Less accurate below Mach 0.5. Inhibited below Mach 0.395.

### 🔧 Additional Notes

* **`acas_ra`**: ACAS Resolution Advisory – *experimental*
* **`gpsOkBefore`**: Timestamp before which GPS was functioning well – *experimental*

> 📝 *Section references such as `(2.2.x)` refer to standards in DO-260B.*

---

## Dates of interest

| Original (GMT+8)             | Converted (UTC)            |
| ---------------------------- | -------------------------- |
| 2025.02.08\_0900-1200\_GMT+8 | 2025.02.08\_0100-0400\_UTC |
| 2025.02.08\_1900-2100\_GMT+8 | 2025.02.08\_1100-1300\_UTC |
| 2025.02.04\_0900-1200\_GMT+8 | 2025.02.04\_0100-0400\_UTC |
| 2025.02.04\_1900-2100\_GMT+8 | 2025.02.04\_1100-1300\_UTC |
| 2025.02.03\_0900-1200\_GMT+8 | 2025.02.03\_0100-0400\_UTC |
| 2025.02.03\_1900-2100\_GMT+8 | 2025.02.03\_1100-1300\_UTC |
| 2024.12.07\_0900-1200\_GMT+8 | 2024.12.07\_0100-0400\_UTC |
| 2024.12.07\_1900-2100\_GMT+8 | 2024.12.07\_1100-1300\_UTC |
| 2024.12.03\_0900-1200\_GMT+8 | 2024.12.03\_0100-0400\_UTC |
| 2024.12.03\_1900-2100\_GMT+8 | 2024.12.03\_1100-1300\_UTC |
| 2024.12.02\_0900-1200\_GMT+8 | 2024.12.02\_0100-0400\_UTC |
| 2024.12.02\_1900-2100\_GMT+8 | 2024.12.02\_1100-1300\_UTC |

---

## 🐍 Python Environment Management (Anaconda + WSL2)

### 🔁 Activate Conda Environment in VSCode Terminal

Run this every time you open VSCode:

```bash
source /home/melodino22/anaconda3/bin/activate
```

### ✅ Optional: Set Conda-Forge as Default Channel

To avoid needing to append `-c conda-forge` on every install:

```bash
conda config --add channels conda-forge
conda config --set channel_priority strict
```

### 🔧 Python and Anaconda on Ubuntu / WSL2

* Python version: `3.12.7`
* conda version: `conda 24.9.2`

* 🔄 **Install multiple Python versions on Ubuntu**:
  [AskUbuntu: Who is deadsnakes and why should I trust them?](https://askubuntu.com/questions/1398568/installing-python-who-is-deadsnakes-and-why-should-i-trust-them)

* 🧩 **Guide to installing Anaconda on WSL2**:
  [Gist by kauffmanes](https://gist.github.com/kauffmanes/5e74916617f9993bc3479f401dfec7da)

---

## Others
Commit messages should follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).


