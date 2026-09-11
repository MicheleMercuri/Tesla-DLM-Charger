# 🚗⚡ Tesla DLM — Dynamic Load Management for Home Assistant

**Intelligent EV charging that follows your solar production, grid headroom, and energy tariffs, fully automated via AppDaemon.**

[![Home Assistant](https://img.shields.io/badge/Home%20Assistant-2024.1+-blue?logo=home-assistant)](https://www.home-assistant.io/)
[![AppDaemon](https://img.shields.io/badge/AppDaemon-4.4+-green)](https://appdaemon.readthedocs.io/)
[![License](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.10+-blue)](https://python.org)
![Version](https://img.shields.io/badge/version-3.11-informational)

---

## What It Does

Tesla DLM dynamically adjusts your Tesla's charging current in real time based on available power from the grid, solar panels, battery storage, or energy dispatch windows, without ever tripping the main breaker.

It's a full Python rewrite of a 187-node Node-RED flow, now running as a single AppDaemon app with clean state management and Telegram integration.

<img width="1626" height="913" alt="Immagine 2026-03-01 202542" src="https://github.com/user-attachments/assets/3d5573e6-f014-4ae5-8ba5-997903dab5b0" />

---

## Charging Modes

| Mode | Description |
|---|---|
| ☀️ **PV DLM** | Follows solar surplus: charges only what the panels produce beyond house consumption |
| 🔌 **Grid DLM** | Follows grid headroom: charges up to the contract limit without overloading the meter |
| 🌙 **Off Peak DLM** | Charges during cheap F3 tariff hours (23:00–07:00 on weekdays, all day on weekends/holidays in Italy) using Grid DLM |
| 🔄 **Inverter DLM** | Follows inverter output and checks the Luna2000 battery SOC before drawing power |
| 🐙 **Octopus DLM** | Charges during Octopus Energy Intelligent Dispatching slots |

---

## Key Features

- **🧠 Smart Auto-Start**: when you plug in without selecting a mode, the app evaluates sun position and PV production and suggests the best mode via Telegram inline keyboard. Auto-starts after 60s if you don't reply.
- **📡 Smart Polling**: Tesla polling activates at sunrise (to save the 12V battery) and turns off at sunset when idle. Always on while charging or away from home.
- **📊 Weekly 100% Tracker**: monitors whether the battery has been charged to 100% in the last 7 days (as recommended by Tesla) and sets the charge target automatically (100% if due, 80% otherwise). Persistent across restarts via an `input_text` helper.
- **🔋 100% charge handled by the car** *(3.11)*: the app writes the real charge limit on the car (`number.model3_charge_limit`), 80% by default and 100% when the weekly full charge is due or requested. With an 80% limit the DLM ends the session; with a 100% limit the car does, so cell balancing above 99% is never cut short.
- **👆 Manual 100% override** *(3.11)*: toggle `input_boolean.tesla_force_100` from the dashboard. It switches itself off when the charge completes and the target goes back to 80%, even if AppDaemon was restarted mid-session.
- **🏠 PV reserve for the home battery** *(3.10)*: optional switch for PV DLM. While the Luna2000 is below its SOC target, the power set in `input_number.tesla_pv_battery_reserve_power` is left to the home battery instead of the car. Off = the car gets the whole surplus.
- **🛑 Hands off when charging away from home** *(3.8)*: at a Supercharger or any other charger the app never changes the current and never turns off a charger it did not start.
- **⚡ Power Manager Integration**: reacts to `sensor.power_manager_zone` to cap charging amps. In the yellow zone it acts in three steps (monitor, 50% reduction after 3 min, full reduction after 45 min) and it answers explicit reduction requests sent by [Power Manager](https://github.com/MicheleMercuri/Power-Manager) v7 through the `pm_request_tesla_reduce` event.
- **🔋 Luna2000 Battery Management**: the home battery SOC floor (`input_number.luna2000_soc_target`) stops the charge in Inverter DLM and, since 3.9, in PV DLM too. Also manages battery discharge power to protect against double drain.
- **📲 Telegram Inline Keyboard**: interactive mode selection with reply buttons; auto-edits messages on timeout or confirmation.
- **📈 Periodic Status Report**: sends a full energy snapshot every 30 minutes while charging (configurable interval).



https://github.com/user-attachments/assets/b059685b-eb45-43b7-965f-d6b65771e42d


---

## How It Works

### PV DLM Loop

```
Every 30s:
  ┌─────────────────────────────┐
  │  Calculate PV surplus        │  pv_input - active + pv_to_grid + wallbox - grid
  │  Subtract PV reserve         │  only if reserve ON and Luna SOC < target
  │  Apply inverter cap          │  surplus = min(surplus, inverter_max)
  │  Convert to amps             │  amps = floor(surplus / voltage)
  │  Clamp to Tesla limits       │  amps = max(min_A, min(amps, max_A))
  │  Skip if stable (dead band)  │  if |Δ| < 1A → no change
  │  Set charging amps           │  number.set_value
  └─────────────────────────────┘
```

### Grid DLM Loop

```
Every 30s:
  ┌─────────────────────────────┐
  │  headroom = meter + grid     │  available margin on contract
  │  available = headroom + wb   │  add back Tesla (reclaimable)
  │  amps = floor(available / V) │  convert to amps
  │  Clamp + dead band check     │  skip if stable
  └─────────────────────────────┘
```

### Auto-Start Decision Tree

```
Charger turned ON without mode selected
        │
        ▼
  Is the car at home?
  ┌─────┴─────┐
 YES          NO → do nothing (public charger)
  │
  Is sun above horizon?
  ┌─────┴─────┐
 YES          NO → Grid DLM
  │
  PV power > threshold?
  ┌─────┴─────┐
 YES          NO → Grid DLM
  │
 PV DLM (auto)
        │
        └── Send Telegram with buttons (60s timeout)
```

### When does the session end?

| Charge limit on the car | Who stops the charge |
|---|---|
| 80% (default) | Tesla DLM, when SOC reaches the target |
| 100% (weekly full charge or manual override) | The car, after cell balancing. Tesla DLM then resets the limit to 80% |

---

## Requirements

- **Home Assistant** 2024.1 or later
- **AppDaemon** 4.4 or later (as HA add-on or standalone)
- **Tesla Custom Integration**, e.g. [Tesla Custom Integration](https://github.com/alandtse/tesla) or any that exposes `switch`, `number`, `sensor`, `device_tracker` for your Tesla (the charge limit `number` is needed since 3.11)
- **Wallbox energy meter**: sensor reporting voltage and power from the charging circuit

### Optional but Recommended

- **Huawei Solar integration**: for PV/Grid/Inverter DLM modes ([Huawei Solar](https://github.com/wlcrs/huawei_solar))
- **Telegram bot**: for push notifications and inline keyboard control (configure via HA `telegram_bot` platform)
- **Octopus Energy integration**: for Octopus DLM mode
- **Power Manager**: any custom sensor that signals load zones (e.g. `sensor.power_manager_zone`), or [Power Manager](https://github.com/MicheleMercuri/Power-Manager) v7 for explicit reduction requests
- **Working Day sensor**: for Italian F1/F2/F3 tariff detection (e.g. [Workday integration](https://www.home-assistant.io/integrations/workday/))

---

## Installation

### 1. Copy the AppDaemon App

```
appdaemon/
└── apps/
    └── tesla_dlm.py
```

### 2. Configure

Copy `apps.yaml.example` to your AppDaemon apps folder, rename it (or add to your existing `apps.yaml`), and fill in your chat ID:

```yaml
tesla_dlm:
  module: tesla_dlm
  class: TeslaDLM
  telegram_chat_id: YOUR_CHAT_ID
```

> **Note:** All entity IDs are defined as constants at the top of `tesla_dlm.py`. Edit them to match your setup before deploying.

### 3. Create Required HA Helpers

The app reads from and writes to several `input_select`, `input_number`, `input_boolean` and `input_text` helpers. Create them in HA (Settings → Helpers) or via YAML:

| Helper | Type | Description |
|---|---|---|
| `input_select.tesla_chargemode_select` | Select | Charging mode selector (Off / PV DLM / Grid DLM / Off Peak DLM / Inverter DLM / Octopus DLM) |
| `input_number.tesla_battery_charge_target` | Number | Target SOC % (e.g. 80) |
| `input_number.electric_meter_power` | Number | Grid contract power in W (e.g. 6000) |
| `input_number.inverter_max_power` | Number | Inverter max output in W |
| `input_number.luna2000_soc_target` | Number | Luna2000 min SOC threshold (Inverter DLM and PV DLM) |
| `input_number.tesla_pv_auto_start_threshold` | Number | PV power (W) needed for auto PV DLM |
| `input_text.tesla_last_100_date` | Text | Persistent storage for last 100% charge date |
| `input_boolean.tesla_force_100` | Boolean | *(3.11)* Manual 100% charge override, turns itself off when done |
| `input_boolean.tesla_pv_battery_reserve` | Boolean | *(3.10)* Keep part of the PV surplus for the home battery |
| `input_number.tesla_pv_battery_reserve_power` | Number | *(3.10)* Power in W reserved for the home battery (e.g. 0–5000, step 100) |

YAML for the helpers added in 3.10 and 3.11:

```yaml
input_boolean:
  tesla_force_100:
    name: Tesla force 100% charge
    icon: mdi:battery-charging-100
  tesla_pv_battery_reserve:
    name: Tesla PV reserve for home battery
    icon: mdi:home-battery-outline

input_number:
  tesla_pv_battery_reserve_power:
    name: Tesla PV reserve power
    min: 0
    max: 5000
    step: 100
    unit_of_measurement: W
    icon: mdi:home-battery
```

### 4. Dashboard (Optional)

Copy `tesla_dlm_dashboard.yaml` into your Lovelace raw config. Requires:

- [Mushroom Cards](https://github.com/piitaya/lovelace-mushroom)
- [card-mod](https://github.com/thomasloven/lovelace-card-mod)

### Upgrading from 3.4

1. Replace `tesla_dlm.py` and re-apply your entity IDs to the constants at the top of the file. New constants: `TESLA_CHARGE_LIMIT`, `FORCE_100_SWITCH`, `PV_BATT_RESERVE_SWITCH`, `PV_BATT_RESERVE_POWER`.
2. Create the three new helpers listed above.
3. Optional: update the dashboard to get the new controls.

---

## Entity Reference

All entities used by the app are defined as module-level constants in `tesla_dlm.py`. The key ones:

### Tesla (via your Tesla integration)
| Constant | Default Entity | Description |
|---|---|---|
| `TESLA_CHARGER` | `switch.model3_charger` | Charger on/off |
| `TESLA_AMPS` | `number.model3_charging_amps` | Charging current |
| `TESLA_BATTERY` | `sensor.model3_battery` | Battery SOC % |
| `TESLA_CHARGE_LIMIT` | `number.model3_charge_limit` | Real charge limit on the car (80/100) |
| `TESLA_POLLING` | `switch.model3_polling` | Polling on/off |
| `TESLA_WAKE_UP` | `button.model3_force_data_update` | Force data update |
| `TESLA_LOCATION` | `device_tracker.model3_location_tracker` | Car location |
| `TESLA_CHARGE_POWER_KW` | `sensor.teslapower_kw_front` | Charge power in kW |
| `TESLA_DATA_UPDATE` | `sensor.model3_data_last_update_time` | Last update timestamp |

### Energy (via Huawei Solar or equivalent)
| Constant | Default Entity | Description |
|---|---|---|
| `PV_INPUT_POWER` | `sensor.input_power` | PV production (W) |
| `INVERTER_ACTIVE_POWER` | `sensor.active_power` | Inverter output (W) |
| `PV_TO_GRID` | `sensor.pv_to_grid_kwp` | Export to grid (W) |
| `POWER_GRID` | `sensor.power_grid_kwp` | Grid import (W) |
| `GRID_ACTIVE_POWER` | `sensor.grid_active_power` | Signed grid power (W, negative = import) |
| `WALLBOX_POWER` | `sensor.wallbox_em_channel_1_power` | Wallbox power (W) |
| `WALLBOX_VOLTAGE` | `sensor.wallbox_em_channel_2_voltage` | Wallbox voltage (V) |
| `LUNA_SOC` | `sensor.battery_state_of_capacity` | Luna2000 SOC % |
| `LUNA_DISCHARGE_POWER` | `number.batteries_potenza_massima_di_scaricamento_batteria` | Luna2000 max discharge |

### Tariffs and other integrations
| Constant | Default Entity | Description |
|---|---|---|
| `TARIFF_BAND` | `sensor.pun_fascia_corrente` | Current tariff band (F1/F2/F3) |
| `WORKING_DAY` | `binary_sensor.working_day_tariff_f1_f2` | Working day (off = F3 all day) |
| `OCTOPUS_DISPATCHING` | `binary_sensor.YOUR_OCTOPUS_DEVICE_ID_dispatching_intelligente_ev` | Octopus dispatch slot active |
| `OCTOPUS_SMART_EV` | `switch.YOUR_OCTOPUS_DEVICE_ID_controllo_smart_ev` | Octopus smart charge control |
| `PM_ZONE_SENSOR` | `sensor.power_manager_zone` | Load zone from Power Manager (green/yellow/red) |

---

## Adapting to Your Setup

### Different Car

Any EV with a HA switch (charger), number (amps), and sensor (SOC) should work. Just update the `TESLA_*` constants at the top of the file.

### No Solar Panels

Set `PV_INPUT_POWER`, `PV_TO_GRID`, `INVERTER_ACTIVE_POWER` to any sensor that returns 0, or use only Grid DLM / Off Peak DLM modes.

### No Luna2000

Leave `LUNA_SOC` and `LUNA_DISCHARGE_POWER` pointing to non-existent entities. The app will use default values (0 for SOC check, skip discharge control). Keep `input_boolean.tesla_pv_battery_reserve` off.

### Different Tariff System

Off Peak DLM uses `TARIFF_BAND` and `WORKING_DAY` sensors to determine F3 hours (23:00–07:00, weekends and holidays in Italy). Adapt the `_is_offpeak_now()` logic to your local tariff structure.

### No Octopus Energy

Octopus DLM activates when `OCTOPUS_DISPATCHING` goes `on`. If you have the integration, replace `YOUR_OCTOPUS_DEVICE_ID` in the two `OCTOPUS_*` constants with the id used by your entities. If you don't, simply never select Octopus DLM mode: it won't affect other modes.

---

## File Structure

```
tesla-dlm/
├── tesla_dlm.py                  # AppDaemon app (main logic)
├── apps.yaml.example             # Configuration template
├── tesla_dlm_dashboard.yaml      # Lovelace dashboard
├── .gitignore
├── LICENSE
└── README.md
```

---

## Telegram Setup

The app uses Home Assistant's native `telegram_bot` integration (polling mode). No direct HTTP calls.

1. Create a Telegram bot via [@BotFather](https://t.me/BotFather) and add the token to `configuration.yaml`:

```yaml
telegram_bot:
  - platform: polling
    api_key: "YOUR_BOT_TOKEN"
    allowed_chat_ids:
      - YOUR_CHAT_ID
```

2. Add your `telegram_chat_id` to `apps.yaml`.

3. The app will automatically send/edit messages and respond to inline keyboard callbacks.

---

## Changelog

### 3.11
- The charge limit on the car is now driven by the app: 80% by default, 100% when the weekly full charge is due or requested.
- With a 100% limit the car ends the session, so balancing is never interrupted; the target goes back to 80% when the charge is complete.
- New manual override `input_boolean.tesla_force_100`.
- A robust cleanup (charger off for 60s at SOC ≥ 99%) resets override and limit even after an AppDaemon restart mid-session.

### 3.10
- PV reserve for the home battery in PV DLM (`input_boolean.tesla_pv_battery_reserve` + `input_number.tesla_pv_battery_reserve_power`). Fixes the case where a 100% target drained all the PV and the home battery stopped charging.

### 3.9
- The Luna2000 SOC floor now stops the charge in PV DLM too, not only in Inverter DLM.

### 3.8
- No interference with charging away from home: position check before touching the current in every start path; the app no longer turns off a charger it did not start.

### 3.7
- Progressive Power Manager yellow-zone handling in 3 steps and listener for the `pm_request_tesla_reduce` event.
- Telegram inline keyboard sent as a nested list (format required by HA when callback data contains separators).

### 3.6
- More robust history reading (defensive checks on malformed entries).

### 3.5
- Fast startup initialization of the weekly 100% sensor from the persistent helper, periodic refresh every 6 hours, robust detection of SOC = 100.

### 3.4
- First public release.

---

## License

MIT, see [LICENSE](LICENSE) for details.

---

## Credits

Built for the Italian energy ecosystem (F1/F2/F3 tariffs, Huawei SUN2000 inverters, Luna2000 batteries).

If you're using this with a different inverter, EV, or tariff system, PRs welcome!
