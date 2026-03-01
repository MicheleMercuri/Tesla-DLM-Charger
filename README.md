# 🚗⚡ Tesla DLM — Dynamic Load Management for Home Assistant

**Intelligent EV charging that follows your solar production, grid headroom, and energy tariffs — fully automated via AppDaemon.**

[![Home Assistant](https://img.shields.io/badge/Home%20Assistant-2024.1+-blue?logo=home-assistant)](https://www.home-assistant.io/)
[![AppDaemon](https://img.shields.io/badge/AppDaemon-4.4+-green)](https://appdaemon.readthedocs.io/)
[![License](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.10+-blue)](https://python.org)

---

## What It Does

Tesla DLM dynamically adjusts your Tesla's charging current in real time based on available power from the grid, solar panels, battery storage, or energy dispatch windows — without ever tripping the main breaker.

It's a full Python rewrite of a 187-node Node-RED flow, now running as a single AppDaemon app with clean state management and Telegram integration.

---

## Charging Modes

| Mode | Description |
|---|---|
| ☀️ **PV DLM** | Follows solar surplus — charges only what the panels produce beyond house consumption |
| 🔌 **Grid DLM** | Follows grid headroom — charges up to the contract limit without overloading the meter |
| 🌙 **Off Peak DLM** | Charges during cheap F3 tariff hours (23:00–07:00 on weekends/holidays in Italy) using Grid DLM |
| 🔄 **Inverter DLM** | Follows inverter output — also checks Luna2000 battery SOC before drawing power |
| 🐙 **Octopus DLM** | Charges during Octopus Energy Intelligent Dispatching slots |

---

## Key Features

- **🧠 Smart Auto-Start** — when you plug in without selecting a mode, the app evaluates sun position and PV production and suggests the best mode via Telegram inline keyboard. Auto-starts after 60s if you don't reply.
- **📡 Smart Polling** — Tesla polling activates at sunrise (to save 12V battery) and turns off at sunset when idle. Always on while charging or away from home.
- **📊 Weekly 100% Tracker** — monitors if the battery has been charged to 100% in the last 7 days (as recommended by Tesla). Adjusts charge target automatically (100% if needed, 80% otherwise). Persistent across restarts via `input_text` helper.
- **⚡ Power Manager Integration** — reacts to a `sensor.power_manager_zone` sensor to cap charging amps and avoid main breaker tripping during high-load events.
- **🔋 Luna2000 Battery Management** — in Inverter DLM mode, limits Tesla charging if the home battery SOC drops below a configurable threshold. Also manages battery discharge power to protect against double drain.
- **📲 Telegram Inline Keyboard** — interactive mode selection with reply buttons; auto-edits messages on timeout or confirmation.
- **📈 Periodic Status Report** — sends a full energy snapshot every 30 minutes while charging (configurable interval).

---

## How It Works

### PV DLM Loop

```
Every 30s:
  ┌─────────────────────────────┐
  │  Calculate PV surplus        │  pv_input - active + pv_to_grid + wallbox - grid
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

---

## Requirements

- **Home Assistant** 2024.1 or later
- **AppDaemon** 4.4 or later (as HA add-on or standalone)
- **Tesla Custom Integration** — e.g. [Tesla Custom Integration](https://github.com/alandtse/tesla) or any that exposes `switch`, `number`, `sensor`, `device_tracker` for your Tesla
- **Wallbox energy meter** — sensor reporting voltage and power from the charging circuit

### Optional but Recommended

- **Huawei Solar integration** — for PV/Grid/Inverter DLM modes ([Huawei Solar](https://github.com/wlcrs/huawei_solar))
- **Telegram bot** — for push notifications and inline keyboard control (configure via HA `telegram_bot` platform)
- **Octopus Energy integration** — for Octopus DLM mode
- **Power Manager** — any custom sensor that signals load zones (e.g. `sensor.power_manager_zone`)
- **Working Day sensor** — for Italian F1/F2/F3 tariff detection (e.g. [Workday integration](https://www.home-assistant.io/integrations/workday/))

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

The app reads from and writes to several `input_select`, `input_number`, and `input_text` helpers. Create them in HA (Settings → Helpers) or via YAML:

| Helper | Type | Description |
|---|---|---|
| `input_select.tesla_chargemode_select` | Select | Charging mode selector (Off / PV DLM / Grid DLM / Off Peak DLM / Inverter DLM / Octopus DLM) |
| `input_number.tesla_battery_charge_target` | Number | Target SOC % (e.g. 80) |
| `input_number.electric_meter_power` | Number | Grid contract power in W (e.g. 6000) |
| `input_number.inverter_max_power` | Number | Inverter max output in W |
| `input_number.luna2000_soc_target` | Number | Luna2000 min SOC threshold for Inverter DLM |
| `input_number.tesla_pv_auto_start_threshold` | Number | PV power (W) needed for auto PV DLM |
| `input_text.tesla_last_100_date` | Text | Persistent storage for last 100% charge date |

### 4. Dashboard (Optional)

Copy `dashboard/tesla_dlm_dashboard.yaml` into your Lovelace raw config. Requires:

- [Mushroom Cards](https://github.com/piitaya/lovelace-mushroom)
- [card-mod](https://github.com/thomasloven/lovelace-card-mod)

---

## Entity Reference

All entities used by the app are defined as module-level constants in `tesla_dlm.py`. The key ones:

### Tesla (via your Tesla integration)
| Constant | Default Entity | Description |
|---|---|---|
| `TESLA_CHARGER` | `switch.model3_charger` | Charger on/off |
| `TESLA_AMPS` | `number.model3_charging_amps` | Charging current |
| `TESLA_BATTERY` | `sensor.model3_battery` | Battery SOC % |
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

---

## Adapting to Your Setup

### Different Car

Any EV with a HA switch (charger), number (amps), and sensor (SOC) should work. Just update the `TESLA_*` constants at the top of the file.

### No Solar Panels

Set `PV_INPUT_POWER`, `PV_TO_GRID`, `INVERTER_ACTIVE_POWER` to any sensor that returns 0 — or use only Grid DLM / Off Peak DLM modes.

### No Luna2000

Leave `LUNA_SOC` and `LUNA_DISCHARGE_POWER` pointing to non-existent entities. The app will use default values (0 for SOC check in Inverter mode, skip discharge control).

### Different Tariff System

Off Peak DLM uses `TARIFF_BAND` and `WORKING_DAY` sensors to determine F3 hours (23:00–07:00, weekends and holidays in Italy). Adapt the `_is_offpeak_now()` logic to your local tariff structure.

### No Octopus Energy

Octopus DLM activates when `OCTOPUS_DISPATCHING` goes `on`. If you don't have this integration, simply never select Octopus DLM mode — it won't affect other modes.

---

## File Structure

```
tesla-dlm/
├── tesla_dlm.py                      # AppDaemon app (main logic)
├── apps.yaml.example                 # Configuration template
├── dashboard/
│   └── tesla_dlm_dashboard.yaml      # Lovelace dashboard
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

## License

MIT — see [LICENSE](LICENSE) for details.

---

## Credits

Migrated from a 187-node Node-RED flow to Python + AppDaemon.  
Built for the Italian energy ecosystem (F1/F2/F3 tariffs, Huawei SUN2000 inverters, Luna2000 batteries).

If you're using this with a different inverter, EV, or tariff system — PRs welcome!
