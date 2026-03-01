"""
Tesla PV DLM - Dynamic Load Management per Tesla Model 3
=========================================================
AppDaemon app per Home Assistant.

Modalità di ricarica:
  - PV DLM:       segue il surplus fotovoltaico
  - Grid DLM:     segue la potenza residua del contatore
  - Off Peak DLM: ricarica in fascia F3 (23-07 / festivi) con Grid DLM
  - Inverter DLM: segue la potenza dell'inverter (con check Luna2000 SOC)
  - Octopus DLM:  ricarica negli slot Octopus Intelligent Dispatching

Funzioni accessorie:
  - Tracking settimanale ultima ricarica al 100%
  - Smart polling (alba/tramonto + posizione auto)
  - Auto-start PV/Grid DLM basato su produzione solare
  - Notifiche Telegram
  - Gestione scarico batteria Luna2000
"""

import appdaemon.plugins.hass.hassapi as hass
from datetime import datetime, timedelta
import math

# ─────────────────────────────────────────────────────────────────────
# CONFIGURAZIONE ENTITÀ HA
# ─────────────────────────────────────────────────────────────────────

# Tesla Model 3
TESLA_CHARGER         = "switch.model3_charger"
TESLA_AMPS            = "number.model3_charging_amps"
TESLA_BATTERY         = "sensor.model3_battery"
TESLA_POLLING         = "switch.model3_polling"
TESLA_WAKE_UP         = "button.model3_force_data_update"
TESLA_LOCATION        = "device_tracker.model3_location_tracker"
TESLA_ENERGY_ADDED    = "sensor.model3_energy_added"
TESLA_CHARGE_POWER_KW = "sensor.teslapower_kw_front"
TESLA_DATA_UPDATE     = "sensor.model3_data_last_update_time"

# Controlli Dashboard
CHARGE_MODE_SELECT    = "input_select.tesla_chargemode_select"
CHARGE_TARGET         = "input_number.tesla_battery_charge_target"
METER_POWER           = "input_number.electric_meter_power"
INVERTER_MAX_POWER    = "input_number.inverter_max_power"
LUNA_SOC_TARGET       = "input_number.luna2000_soc_target"
PV_AUTO_START_INPUT   = "input_number.tesla_pv_auto_start_threshold"
LAST_100_HELPER       = "input_text.tesla_last_100_date"  # Persistenza data ultima carica 100%

# Sensori Energia
PV_INPUT_POWER        = "sensor.input_power"
INVERTER_ACTIVE_POWER = "sensor.active_power"
PV_TO_GRID            = "sensor.pv_to_grid_kwp"
POWER_GRID            = "sensor.power_grid_kwp"
GRID_ACTIVE_POWER     = "sensor.grid_active_power"
WALLBOX_POWER         = "sensor.wallbox_em_channel_1_power"
WALLBOX_VOLTAGE       = "sensor.wallbox_em_channel_2_voltage"
LINE_VOLTAGE_AB       = "sensor.a_b_line_voltage"

# Batteria Luna2000
LUNA_SOC              = "sensor.battery_state_of_capacity"
LUNA_DISCHARGE_POWER  = "number.batteries_potenza_massima_di_scaricamento_batteria"

# Tariffe e Octopus
TARIFF_BAND           = "sensor.pun_fascia_corrente"
WORKING_DAY           = "binary_sensor.working_day_tariff_f1_f2"
OCTOPUS_DISPATCHING   = "binary_sensor.YOUR_OCTOPUS_DEVICE_ID_dispatching_intelligente_ev"
OCTOPUS_SMART_EV      = "switch.YOUR_OCTOPUS_DEVICE_ID_controllo_smart_ev"

# Sistema
HA_UPTIME             = "sensor.uptime"

# Power Manager
PM_ZONE_SENSOR        = "sensor.power_manager_zone"

# Costanti
LUNA_DISCHARGE_OFF    = 400     # W - scarica minima (quasi bloccata)
LUNA_DISCHARGE_FULL   = 5000    # W - scarica piena

# Telegram e status report
STATUS_REPORT_INTERVAL = 1800   # secondi (30 minuti)
TG_HEADER              = "🚗⚡ *Tesla DLM:*"

# Auto-start DLM intelligente
AUTO_GRID_DLM_TIMEOUT  = 60     # secondi prima di avviare DLM automatico
DATA_FRESHNESS_MAX     = 90     # secondi max per considerare dati aggiornati
PV_AUTO_START_MIN      = 500    # W - soglia PV per auto-start PV DLM (sotto → Grid DLM)
SUN_ENTITY             = "sun.sun"  # above_horizon / below_horizon


class TeslaDLM(hass.Hass):
    """App principale per la gestione dinamica della ricarica Tesla."""

    def initialize(self):
        """Inizializzazione dell'app e registrazione dei listener."""

        self.log("🚗⚡ Tesla DLM v3.2 - Avvio...")

        # ── Telegram (via servizi HA nativi) ──
        self.telegram_chat_id = self.args.get("telegram_chat_id", None)

        # Handle del loop DLM attivo (per poterlo cancellare al cambio modalità)
        self._dlm_handle = None
        # Handle del countdown off-peak
        self._offpeak_countdown_handle = None
        self._offpeak_trigger_handle = None
        # Handle status report periodico (ogni 30 min)
        self._status_report_handle = None
        # Tracking energia: valore iniziale di energy_added alla partenza
        self._energy_at_start = None
        self._charge_start_time = None
        # Flag per evitare doppie esecuzioni
        self._startup_running = False
        # Flag anti-duplicazione notifica stop
        self._stop_notified = False
        # Handle auto-start Grid DLM (quando charger parte senza selezione)
        self._auto_grid_handle = None
        # Telegram inline keyboard: event listener per callback
        self._tg_sent_listener = None        # handle listen_event sent (per catturare msg_id)
        self._tg_choice_message_id = None    # message_id del messaggio con pulsanti
        self._tg_choice_pending = False       # True quando aspettiamo una scelta utente
        # Power Manager: limite ampere imposto da PM per evitare distacco
        self._pm_throttle_amps = None         # None = nessun limite PM attivo

        # ── LISTENER: Cambio modalità di ricarica ──
        self.listen_state(
            self._on_mode_changed,
            CHARGE_MODE_SELECT
        )

        # ── LISTENER: Octopus Intelligent Dispatching on ──
        self.listen_state(
            self._on_octopus_dispatching,
            OCTOPUS_DISPATCHING,
            new="on",
            old="off",
            duration=15   # ritardo per evitare falsi positivi
        )

        # ── LISTENER: Charger acceso senza selezione → auto Grid DLM ──
        self.listen_state(
            self._on_charger_turned_on,
            TESLA_CHARGER,
            new="on",
            old="off"
        )

        # ── POLLING: gestione smart basata su posizione auto ──
        # A casa: ON alba → OFF tramonto (risparmia batteria 12V)
        # Fuori casa: sempre ON (tracking posizione)
        # In ricarica: sempre ON
        self.run_at_sunrise(self._on_sunrise)
        self.run_at_sunset(self._on_sunset)
        self.listen_state(self._on_location_changed, TESLA_LOCATION)

        # ── TELEGRAM: listener permanente per callback inline keyboard ──
        self.listen_event(self._on_telegram_callback, "telegram_callback")
        self.log("  TG: listen_event('telegram_callback') registrato")

        # ── POWER MANAGER: reagisci ai cambi zona per evitare distacco ──
        self.listen_state(self._on_pm_zone_change, PM_ZONE_SENSOR)
        self.log("  PM: listen_state su sensor.power_manager_zone")

        # ── STARTUP: aggiorna sensore 100% ──
        self.run_in(self._on_startup_check, 120)  # 2 minuti dopo avvio

        self.log("🚗⚡ Tesla DLM v3.2 - Pronto!")
        self.log(f"  Telegram:    HA nativo (chat_id: {self.telegram_chat_id})")

    # =====================================================================
    # UTILITÀ COMUNI
    # =====================================================================

    def _get_float(self, entity_id, attribute=None, default=0.0):
        """Legge un valore float da un'entità HA, con default sicuro."""
        try:
            if attribute:
                val = self.get_state(entity_id, attribute=attribute)
            else:
                val = self.get_state(entity_id)
            return float(val) if val is not None else default
        except (ValueError, TypeError):
            return default

    def _get_amp_limits(self):
        """Restituisce (min_amp, max_amp) dalla wallbox/Tesla."""
        min_amp = self._get_float(TESLA_AMPS, attribute="min", default=1)
        max_amp = self._get_float(TESLA_AMPS, attribute="max", default=16)
        return int(min_amp), int(max_amp)

    def _watts_to_amps(self, available_watts):
        """
        Converte watt disponibili in ampere per la Tesla.
        Usa il voltaggio reale dalla wallbox e clampa ai limiti.
        """
        voltage = self._get_float(WALLBOX_VOLTAGE, default=230.0)
        min_amp, max_amp = self._get_amp_limits()

        if voltage <= 0:
            voltage = 230.0

        # Calcola ampere: floor(watt / volt) per non superare la potenza
        amps = int(available_watts / voltage)

        # Clamp ai limiti
        amps = max(min_amp, min(amps, max_amp))

        return amps

    def _should_continue_charging(self, mode=None):
        """
        Verifica se la ricarica deve continuare.

        Returns: True = continua, False = ferma
        """
        target = self._get_float(CHARGE_TARGET, default=80)
        soc = self._get_float(TESLA_BATTERY, default=0)

        # Target 100%: carica sempre (fino a quando Tesla non dice basta)
        if target == 100:
            return True

        # Per la modalità Inverter DLM: check anche Luna2000 SOC
        if mode == "Inverter DLM":
            luna_target = self._get_float(LUNA_SOC_TARGET, default=20)
            luna_soc = self._get_float(LUNA_SOC, default=100)
            if luna_soc <= luna_target:
                self.log(f"🔋 Luna2000 SOC {luna_soc}% ≤ target {luna_target}% → STOP")
                return False

        # Target raggiunto → stop
        if soc >= target:
            self.log(f"🔋 Tesla SOC {soc}% ≥ target {target}% → STOP")
            return False

        return True

    def _is_charger_on(self):
        """Verifica se il caricatore Tesla è attivo."""
        return self.get_state(TESLA_CHARGER) == "on"

    def _is_mode_active(self, expected_mode):
        """Verifica se la modalità corrente corrisponde a quella attesa."""
        return self.get_state(CHARGE_MODE_SELECT) == expected_mode

    def _is_data_fresh(self):
        """
        Verifica che i dati Tesla siano aggiornati (timestamp entro DATA_FRESHNESS_MAX secondi).
        Usa sensor.model3_data_last_update_time.
        """
        try:
            last_update_str = self.get_state(TESLA_DATA_UPDATE)
            if not last_update_str or last_update_str in ("unknown", "unavailable"):
                self.log("⚠️ Timestamp Tesla non disponibile")
                return False

            from datetime import datetime, timezone
            # Parsa il timestamp ISO (es: 2026-02-27T15:10:05+00:00)
            last_update = datetime.fromisoformat(last_update_str)
            now = datetime.now(timezone.utc)
            delta = (now - last_update).total_seconds()

            self.log(f"📡 Freshness check: ultimo update {delta:.0f}s fa (max {DATA_FRESHNESS_MAX}s)")
            return delta <= DATA_FRESHNESS_MAX
        except Exception as e:
            self.log(f"⚠️ Errore freshness check: {e}")
            return False

    def _cancel_auto_grid(self):
        """Cancella eventuale timer auto-start Grid DLM e listener Telegram."""
        if self._auto_grid_handle:
            self.cancel_timer(self._auto_grid_handle)
            self._auto_grid_handle = None
        self._stop_sent_listener()

    # ─────────────────────────────────────────────────────────────────
    # AUTO-START GRID DLM (charger acceso senza selezione)
    # con scelta via Telegram Inline Keyboard
    # ─────────────────────────────────────────────────────────────────

    def _on_charger_turned_on(self, entity, attribute, old, new, kwargs):
        """
        Callback quando switch.model3_charger passa da off→on.
        Se nessuna modalità è selezionata, valuta automaticamente:
        - Sole alto + PV > soglia → PV DLM
        - Sole tramontato → Grid DLM
        Invia notifica Telegram con pulsanti per override.
        Timeout AUTO_GRID_DLM_TIMEOUT secondi → avvio automatico.
        """
        current_mode = self.get_state(CHARGE_MODE_SELECT)
        if current_mode != "Off":
            return

        # Determina modalità suggerita
        auto_mode, auto_reason = self._evaluate_auto_mode()

        self.log(
            f"🔌 Charger acceso senza modalità → suggerito: "
            f"{auto_mode} ({auto_reason})"
        )

        emoji = self._get_mode_emoji(auto_mode)
        self._send_telegram_with_keyboard(
            f"{TG_HEADER}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔌 *Ricarica rilevata!*\n\n"
            f"Nessuna modalità DLM selezionata.\n"
            f"Suggerita: {emoji} *{auto_mode}*\n"
            f"_{auto_reason}_\n\n"
            f"Scegli o attendi {AUTO_GRID_DLM_TIMEOUT}s per avvio auto.",
            keyboard=[
                "☀️ PV DLM:/dlm_pv, 🔌 Grid DLM:/dlm_grid",
                "🌙 Off Peak:/dlm_offpeak, 🔄 Inverter:/dlm_inverter",
                "❌ Annulla (spegni):/dlm_off",
            ]
        )

        # Timer timeout → avvio automatico modalità suggerita
        self._cancel_auto_grid()
        self._auto_grid_handle = self.run_in(
            self._auto_dlm_timeout,
            AUTO_GRID_DLM_TIMEOUT
        )

    def _evaluate_auto_mode(self):
        """
        Valuta quale modalità DLM avviare automaticamente.
        Ritorna (mode, reason).
        Soglia PV letta da dashboard (input_number.tesla_pv_auto_start_threshold).
        """
        sun_up = self._is_sun_up()
        pv_power = self._get_float(PV_INPUT_POWER)
        pv_threshold = self._get_float(PV_AUTO_START_INPUT, default=PV_AUTO_START_MIN)

        if sun_up and pv_power >= pv_threshold:
            return "PV DLM", f"Sole alto, PV {pv_power:.0f}W (soglia {pv_threshold:.0f}W)"
        elif sun_up and pv_power < pv_threshold:
            return "Grid DLM", f"Sole alto ma PV bassa ({pv_power:.0f}W < {pv_threshold:.0f}W)"
        else:
            return "Grid DLM", "Sole tramontato"

    def _auto_dlm_timeout(self, kwargs):
        """Scaduto il timeout: avvia modalità auto (PV o Grid in base al sole)."""
        self._auto_grid_handle = None

        # Disattiva ricezione callback (scelta scaduta)
        self._tg_choice_pending = False

        current_mode = self.get_state(CHARGE_MODE_SELECT)
        if current_mode != "Off":
            self.log(f"⏱️ Auto DLM: modalità già selezionata ({current_mode})")
            return

        if not self._is_charger_on():
            self.log("⏱️ Auto DLM: charger spento nel frattempo")
            self._edit_telegram_choice_message("⏱️ *Timeout* — Charger già spento")
            return

        # Rivaluta al momento del timeout (il sole potrebbe essersi mosso)
        auto_mode, auto_reason = self._evaluate_auto_mode()
        emoji = self._get_mode_emoji(auto_mode)

        self.log(f"⏱️ Nessuna scelta → avvio {auto_mode} ({auto_reason})")

        # Aggiorna messaggio Telegram
        self._edit_telegram_choice_message(
            f"{TG_HEADER}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱️ {emoji} *{auto_mode} Automatico*\n\n"
            f"Nessuna scelta entro {AUTO_GRID_DLM_TIMEOUT}s.\n"
            f"{auto_reason}"
        )

        self._set_charge_mode(auto_mode)

    # ─────────────────────────────────────────────────────────────────
    # TELEGRAM: Inline Keyboard + Callback via Servizi HA nativi
    # ─────────────────────────────────────────────────────────────────
    # Usa SOLO self.call_service("telegram_bot/...") per evitare
    # conflitti con il polling HA (errore 409 getUpdates).
    #
    # REQUISITO: in HA deve essere configurata la piattaforma
    #   telegram_bot (polling), con allowed_chat_ids incluso il tuo chat_id
    #
    # Formato inline_keyboard HA: "Label:/callback, Label:/callback"
    # Callback data arriva in evento telegram_callback:
    #   data["data"]    = "/dlm_pv"
    #   data["command"] = "/dlm_pv"
    #   data["id"]      = callback_query_id
    #   data["message"]["message_id"] = msg_id per edit
    #
    # Il listener telegram_callback è registrato PERMANENTEMENTE
    # in initialize(). Il flag _tg_choice_pending
    # controlla se i callback vengono processati o ignorati.
    # ─────────────────────────────────────────────────────────────────

    # Mappa callback_data → nome modalità
    CALLBACK_MAP = {
        "/dlm_pv": "PV DLM",
        "/dlm_grid": "Grid DLM",
        "/dlm_offpeak": "Off Peak DLM",
        "/dlm_inverter": "Inverter DLM",
        "/dlm_off": "Off",
    }

    def _send_telegram_with_keyboard(self, text, keyboard):
        """
        Invia messaggio Telegram con InlineKeyboard via servizio HA.
        keyboard = lista di stringhe formato HA: ["Label:/cb, Label:/cb", ...]
        Il message_id viene catturato automaticamente dall'evento telegram_sent.
        """
        # Avvia ascolto telegram_sent PRIMA di inviare (per catturare msg_id)
        self._start_sent_listener()
        self._tg_choice_pending = True
        try:
            self.call_service(
                "telegram_bot/send_message",
                message=text,
                inline_keyboard=keyboard,
            )
            self.log("  TG: Inline keyboard inviata via HA")
            return True
        except Exception as e:
            self.log(f"TG keyboard errore: {e}", level="WARNING")
            self._stop_sent_listener()
            self._tg_choice_pending = False
        return None

    def _start_sent_listener(self):
        """Ascolta telegram_sent per catturare il message_id del messaggio appena inviato."""
        self._stop_sent_listener()
        self._tg_sent_listener = self.listen_event(
            self._on_telegram_sent,
            "telegram_sent"
        )

    def _stop_sent_listener(self):
        """Ferma ascolto telegram_sent."""
        if self._tg_sent_listener:
            self.cancel_listen_event(self._tg_sent_listener)
            self._tg_sent_listener = None

    def _on_telegram_sent(self, event_name, data, kwargs):
        """Cattura message_id dall'evento telegram_sent dopo invio keyboard."""
        msg_id = data.get("message_id")
        if msg_id:
            self._tg_choice_message_id = msg_id
            self.log(f"  TG: Catturato msg_id: {msg_id} (da telegram_sent)")
        self._stop_sent_listener()

    def _edit_telegram_choice_message(self, new_text, msg_id=None, chat_id=None):
        """Modifica il messaggio con pulsanti: rimuove keyboard e aggiorna testo."""
        mid = msg_id or self._tg_choice_message_id
        cid = chat_id or self.telegram_chat_id
        if not mid:
            self.log("  TG: edit skip (nessun message_id)", level="WARNING")
            return
        try:
            self.call_service(
                "telegram_bot/edit_message",
                chat_id=cid,
                message_id=mid,
                message=new_text,
            )
            self.log(f"  TG: Messaggio {mid} aggiornato")
        except Exception as e:
            self.log(f"TG edit errore: {e}", level="WARNING")
        finally:
            self._tg_choice_message_id = None

    def _answer_callback_query(self, callback_query_id, text=""):
        """Risponde al callback query (toglie l'orologino sul pulsante)."""
        try:
            self.call_service(
                "telegram_bot/answer_callback_query",
                callback_query_id=str(callback_query_id),
                message=text,
                show_alert=False,
            )
        except Exception:
            pass

    def _on_telegram_callback(self, event_name, data, kwargs):
        """
        Evento HA 'telegram_callback' — listener PERMANENTE.
        Processa solo se _tg_choice_pending è True.
        data["data"]    = "/dlm_pv" etc.
        data["id"]      = callback_query_id
        data["message"]["message_id"] = id per edit
        """
        cb_data = data.get("data", "")

        # Ignora callback non nostri
        if cb_data not in self.CALLBACK_MAP:
            return

        cb_id = data.get("id", "")

        # Se non c'è scelta pendente, rispondi comunque (toglie orologino)
        if not self._tg_choice_pending:
            self._answer_callback_query(cb_id, "⏱️ Scelta scaduta")
            self.log(f"  TG: Callback {cb_data} ignorato (nessuna scelta pendente)")
            return

        mode = self.CALLBACK_MAP[cb_data]
        # Aggiorna message_id dal callback
        msg = data.get("message", {})
        if isinstance(msg, dict) and msg.get("message_id"):
            self._tg_choice_message_id = msg["message_id"]
        self.log(f"  TG: Callback HA → {mode} (id: {cb_id}, msg: {self._tg_choice_message_id})")
        self._tg_choice_pending = False
        self._handle_telegram_choice(mode, cb_id)

    def _handle_telegram_choice(self, mode, callback_query_id):
        """
        Processa la scelta fatta via Telegram inline keyboard.
        mode = "PV DLM" | "Grid DLM" | "Off Peak DLM" | "Inverter DLM" | "Off"
        """
        # 1. Rispondi al callback (toglie orologino)
        emoji = self._get_mode_emoji(mode) if mode != "Off" else "❌"
        self._answer_callback_query(callback_query_id, f"{emoji} {mode}")

        # 2. Cancella timer auto-grid
        self._cancel_auto_grid()

        # 3. Aggiorna il messaggio Telegram con la scelta
        if mode == "Off":
            self._edit_telegram_choice_message(
                f"{TG_HEADER}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"❌ *Ricarica annullata*\n\n"
                f"Charger verrà spento."
            )
            # Spegni charger
            self._charger_off()
            self.log("  TG: Utente ha scelto Annulla → charger off")
        else:
            self._edit_telegram_choice_message(
                f"{TG_HEADER}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"{emoji} *{mode}* selezionato via Telegram\n\n"
                f"Avvio sequenza di ricarica..."
            )
            self.log(f"  TG: Utente ha scelto {mode} → avvio")
            self._set_charge_mode(mode)

    def _check_needs_adjustment_grid(self):
        """
        Verifica se serve ricalcolare gli ampere (Grid).
        Formula: headroom = meter + grid_active (SENZA wallbox!)
        
        grid_active_power è NEGATIVO quando si preleva dalla rete.
        headroom = quanto margine resta sul contatore.
        
        Returns: True se serve ricalcolare, False se stabile.
        """
        grid = self._get_float(GRID_ACTIVE_POWER)
        meter = self._get_float(METER_POWER, default=6000)
        voltage = self._get_float(WALLBOX_VOLTAGE, default=230.0)
        current_amps = self._get_float(TESLA_AMPS, default=0)
        min_amp, max_amp = self._get_amp_limits()

        if voltage <= 0:
            voltage = 230.0

        # headroom = meter + grid (NO wallbox!) → quanto margine resta sul contatore
        headroom = meter + grid
        # z = headroom - 1A_in_watt → residuo dopo aver tolto un ampere di sicurezza
        z = headroom - voltage

        if z > 0 and z > voltage and current_amps < max_amp:
            return True   # ho margine per aumentare → ricalcola
        if z > 0 and z > voltage and current_amps >= max_amp:
            return False  # ho margine ma sono già al max
        if z > 0 and z < voltage:
            return False  # margine insufficiente per +1A
        if z < 0 and current_amps <= min_amp:
            return False  # sto sforando ma sono già al minimo
        if -voltage <= z <= voltage:
            return False  # nella banda morta ±1A → stabile
        # else: z molto negativo → sto sforando il contatore → ricalcola
        return True

    def _check_needs_adjustment_pv(self):
        """
        Verifica se serve ricalcolare gli ampere (PV).
        
        Formula CHECK: y = pv_input - active + pv_to_grid - power_grid (SENZA wallbox!)
        z = y - voltage (1 ampere in watt)
        
        Stessa logica condizionale di Check Grid:
        - z > 0 e z > volt e amp < max → ricalcola (surplus per +1A)
        - z molto negativo → ricalcola (serve ridurre)
        - banda morta ±volt → stabile
        """
        pv_input = self._get_float(PV_INPUT_POWER)
        active = self._get_float(INVERTER_ACTIVE_POWER)
        pv_to_grid = self._get_float(PV_TO_GRID)
        power_grid = self._get_float(POWER_GRID)
        max_inverter = self._get_float(INVERTER_MAX_POWER, default=10000)
        voltage = self._get_float(WALLBOX_VOLTAGE, default=230.0)
        current_amps = self._get_float(TESLA_AMPS, default=0)
        min_amp, max_amp = self._get_amp_limits()

        if voltage <= 0:
            voltage = 230.0

        # Formula CHECK (NO wallbox!)
        y = pv_input - active + pv_to_grid - power_grid

        # Limite inverter
        if y > max_inverter:
            y = max_inverter

        z = y - voltage

        if z > 0 and z > voltage and current_amps < max_amp:
            return True   # surplus per +1A → ricalcola
        if z > 0 and z > voltage and current_amps >= max_amp:
            return False  # surplus ma già al max
        if z > 0 and z < voltage:
            return False  # surplus insufficiente per +1A
        if z < 0 and current_amps <= min_amp:
            return False  # deficit ma già al minimo
        if -voltage <= z <= voltage:
            return False  # banda morta ±1A → stabile
        # else: z molto negativo → deficit → ricalcola
        return True

    def _check_needs_adjustment_inverter(self):
        """Check Inverter: stessa logica di Grid ma basata su potenza inverter."""
        grid = self._get_float(GRID_ACTIVE_POWER)
        active = self._get_float(INVERTER_ACTIVE_POWER)
        max_inv = self._get_float(INVERTER_MAX_POWER, default=10000)
        voltage = self._get_float(WALLBOX_VOLTAGE, default=230.0)
        current_amps = self._get_float(TESLA_AMPS, default=0)
        min_amp, max_amp = self._get_amp_limits()

        if voltage <= 0:
            voltage = 230.0

        # headroom sulla potenza inverter (senza wallbox)
        headroom = max_inv + grid - active
        z = headroom - voltage

        if z > 0 and z > voltage and current_amps < max_amp:
            return True
        if z > 0 and z > voltage and current_amps >= max_amp:
            return False
        if z > 0 and z < voltage:
            return False
        if z < 0 and current_amps <= min_amp:
            return False
        if -voltage <= z <= voltage:
            return False
        return True

    # =====================================================================
    # CALCOLI POTENZA DISPONIBILE PER TESLA (formula RICALCOLO = + wallbox)
    # =====================================================================

    def _calc_grid_available(self):
        """
        Calcola la potenza grid disponibile per la ricarica.
        Formula: meter + grid_active + wallbox
        
        Significato: potenza_contatore - consumi_casa = disponibile per Tesla
        - meter = limite contrattuale (es. 6000W)
        - grid_active = negativo quando si importa (es. -4000W = importo 4000W)
        - wallbox = potenza attuale Tesla (va AGGIUNTA perché è "riallocabile")
        
        Esempio: meter=6000, grid=-4000, wallbox=2000
        → casa usa 4000-2000=2000W da rete
        → disponibile per Tesla = 6000-2000 = 4000W
        → formula: 6000+(-4000)+2000 = 4000W ✓
        """
        grid = self._get_float(GRID_ACTIVE_POWER)
        wallbox = self._get_float(WALLBOX_POWER)
        meter = self._get_float(METER_POWER, default=6000)

        available = meter + grid + wallbox

        self.log(f"  📊 Grid calc: meter={meter:.0f} + grid={grid:.0f} + wallbox={wallbox:.0f} = {available:.0f}W")
        return available

    def _calc_pv_surplus(self):
        """
        Calcola il surplus PV disponibile per la ricarica.
        Formula: pv_input - active_power + pv_to_grid + wallbox - power_grid
        
        - pv_input = produzione PV totale
        - active = output inverter (carico casa + batteria)
        - pv_to_grid = eccesso che va in rete (spreco recuperabile)
        - wallbox = potenza attuale Tesla (riallocabile come Grid)
        - power_grid = potenza prelevata da rete (da sottrarre)
        
        Con potenza massima inverter.
        """
        pv_input = self._get_float(PV_INPUT_POWER)
        active = self._get_float(INVERTER_ACTIVE_POWER)
        pv_to_grid = self._get_float(PV_TO_GRID)
        wallbox = self._get_float(WALLBOX_POWER)
        power_grid = self._get_float(POWER_GRID)
        max_inverter = self._get_float(INVERTER_MAX_POWER, default=10000)

        surplus = pv_input - active + pv_to_grid + wallbox - power_grid

        # Limite potenza inverter
        if surplus > max_inverter:
            surplus = max_inverter

        self.log(f"  📊 PV calc: pv={pv_input:.0f} - active={active:.0f} + toGrid={pv_to_grid:.0f} + wb={wallbox:.0f} - grid={power_grid:.0f} = {surplus:.0f}W")
        return surplus

    def _calc_inverter_available(self):
        """
        Calcola la potenza disponibile dall'inverter.
        Formula: inverter_max + grid_active - active_power + wallbox
        Cappato a inverter_max.
        
        Stessa logica Grid: wallbox va AGGIUNTA perché riallocabile.
        """
        grid = self._get_float(GRID_ACTIVE_POWER)
        active = self._get_float(INVERTER_ACTIVE_POWER)
        wallbox = self._get_float(WALLBOX_POWER)
        max_inv = self._get_float(INVERTER_MAX_POWER, default=10000)

        available = max_inv + grid - active + wallbox

        if available > max_inv:
            available = max_inv

        self.log(f"  📊 Inv calc: max={max_inv:.0f} + grid={grid:.0f} - active={active:.0f} + wb={wallbox:.0f} = {available:.0f}W")
        return available

    # =====================================================================
    # AZIONI HA
    # =====================================================================

    def _set_charging_amps(self, amps):
        """Imposta gli ampere di ricarica Tesla. Applica cap PM se attivo."""
        # Applica limite Power Manager (protezione distacco)
        if self._pm_throttle_amps is not None and amps > self._pm_throttle_amps:
            self.log(f"⚡ PM cap: {amps}A → {self._pm_throttle_amps}A (protezione distacco)")
            amps = self._pm_throttle_amps

        current = self._get_float(TESLA_AMPS, default=-1)
        if int(current) == int(amps):
            self.log(f"⚡ Ampere Tesla già a {amps}A → skip")
            return
        self.call_service(
            "number/set_value",
            entity_id=TESLA_AMPS,
            value=amps
        )
        self.log(f"⚡ Ampere Tesla → {amps}A (era {int(current)}A)")

    def _charger_on(self):
        """Accende il caricatore Tesla."""
        self.call_service("switch/turn_on", entity_id=TESLA_CHARGER)
        self.log("🔌 Charger ON")

    def _charger_off(self):
        """Spegne il caricatore Tesla."""
        self.call_service("switch/turn_off", entity_id=TESLA_CHARGER)
        self.log("🔌 Charger OFF")

    def _polling_on(self, reason=""):
        """Attiva il polling Tesla."""
        self.call_service("switch/turn_on", entity_id=TESLA_POLLING)
        self.log(f"📡 Polling ON ({reason})")

    def _polling_off(self, reason=""):
        """Disattiva il polling Tesla."""
        self.call_service("switch/turn_off", entity_id=TESLA_POLLING)
        self.log(f"📡 Polling OFF ({reason})")

    def _is_car_home(self):
        """Verifica se la Tesla è a casa."""
        return self.get_state(TESLA_LOCATION) == "home"

    def _is_sun_up(self):
        """Verifica se il sole è sopra l'orizzonte."""
        return self.get_state(SUN_ENTITY) == "above_horizon"

    def _is_charging_active(self):
        """Verifica se una modalità DLM è attiva (ricarica in corso)."""
        mode = self.get_state(CHARGE_MODE_SELECT)
        return mode is not None and mode != "Off"

    def _on_sunrise(self, kwargs=None):
        """Alba: accendi polling se auto a casa."""
        if self._is_car_home():
            self._polling_on("alba, auto a casa")
        else:
            self.log("📡 Alba: auto fuori casa, polling già attivo")

    def _on_sunset(self, kwargs=None):
        """Tramonto: spegni polling se auto a casa e NON in ricarica."""
        if not self._is_car_home():
            self.log("📡 Tramonto: auto fuori casa, polling resta attivo")
            return
        if self._is_charging_active():
            self.log("📡 Tramonto: ricarica attiva, polling resta attivo")
            return
        self._polling_off("tramonto, auto a casa, no ricarica")

    def _on_location_changed(self, entity, attribute, old, new, kwargs):
        """
        Cambio posizione Tesla:
        - Esce di casa → polling ON sempre
        - Torna a casa → dipende da sole/ricarica
        """
        if old == new:
            return
        self.log(f"📍 Tesla: {old} → {new}")

        if new != "home":
            # Auto uscita da casa → polling sempre attivo fuori casa
            self._polling_on(f"auto fuori casa ({new})")
        else:
            # Auto tornata a casa
            if self._is_charging_active():
                self._polling_on("auto tornata, ricarica attiva")
            elif self._is_sun_up():
                self._polling_on("auto tornata, giorno")
            else:
                self._polling_off("auto tornata, notte, no ricarica")

    def _evaluate_polling_after_charge_stop(self):
        """
        Dopo stop ricarica: valuta se spegnere polling.
        Auto a casa + notte → OFF. Altrimenti → resta ON.
        """
        if not self._is_car_home():
            self.log("📡 Post-stop: auto fuori casa, polling resta attivo")
            return
        if self._is_sun_up():
            self.log("📡 Post-stop: giorno, polling resta attivo")
            return
        self._polling_off("fine ricarica, notte, auto a casa")

    def _force_wake_up(self):
        """Forza aggiornamento dati Tesla."""
        self.call_service("button/press", entity_id=TESLA_WAKE_UP)
        self.log("🔔 Force Wake Up Tesla")

    def _set_charge_target(self, value):
        """Imposta il target di ricarica %."""
        self.call_service(
            "input_number/set_value",
            entity_id=CHARGE_TARGET,
            value=value
        )
        self.log(f"🎯 Target ricarica → {value}%")

    def _set_charge_mode(self, mode):
        """Imposta la modalità di ricarica."""
        self.call_service(
            "input_select/select_option",
            entity_id=CHARGE_MODE_SELECT,
            option=mode
        )

    def _set_luna_discharge(self, watts):
        """Imposta la potenza di scarica Luna2000."""
        self.call_service(
            "number/set_value",
            entity_id=LUNA_DISCHARGE_POWER,
            value=watts
        )
        label = "bloccata" if watts <= LUNA_DISCHARGE_OFF else f"{watts}W"
        self.log(f"🔋 Luna2000 scarica → {label}")

    def _send_telegram(self, message):
        """
        Invia messaggio Telegram via servizio HA nativo.
        Richiede integrazione telegram_bot configurata in HA.
        """
        try:
            self.call_service(
                "telegram_bot/send_message",
                message=message,
            )
            self.log(f"  TG: {message[:60]}...")
        except Exception as e:
            self.log(f"TG errore: {e}", level="WARNING")

    # ─────────────────────────────────────────────────────────────────
    # POWER MANAGER INTEGRATION: protezione distacco
    # ─────────────────────────────────────────────────────────────────
    # Il DLM reagisce ai cambi zona del Power Manager:
    # - VERDE: nessun limite, ricarica normale
    # - GIALLA: riduce ampere per rientrare in verde
    # - ROSSA: riduce al minimo (5A), PM Shelly resta backup hardware
    # ─────────────────────────────────────────────────────────────────

    def _on_pm_zone_change(self, entity, attribute, old, new, kwargs):
        """Reagisce ai cambi zona del Power Manager."""
        if new == old:
            return

        # Ignora se non stiamo ricaricando
        mode = self.get_state(CHARGE_MODE_SELECT)
        if not mode or mode == "Off":
            self._pm_throttle_amps = None
            return

        if not self._is_charger_on():
            self._pm_throttle_amps = None
            return

        self.log(f"🔰 PM zona: {old} → {new}")

        if new == "green":
            self._pm_on_green(old)
        elif new == "yellow":
            self._pm_on_yellow()
        elif new == "red":
            self._pm_on_red()

    def _pm_on_green(self, old_zone):
        """PM torna verde: rilascia il limite, il loop DLM normale riprende."""
        if self._pm_throttle_amps is not None:
            self.log(f"🟢 PM verde: rilascio cap {self._pm_throttle_amps}A → DLM libero")
            self._pm_throttle_amps = None
            self._send_telegram(
                f"{TG_HEADER} 🟢\n"
                f"✅ *Power Manager: rientro in verde*\n"
                f"Limite rimosso, ricarica normale"
            )

    def _pm_on_yellow(self):
        """PM zona gialla: calcola ampere sicuri per rientrare in verde."""
        safe_amps = self._calc_pm_safe_amps()
        current_amps = self._get_float(TESLA_AMPS, default=0)

        if safe_amps >= current_amps:
            # Non serve ridurre, la wallbox non è il problema
            self.log(f"🟡 PM gialla: safe={safe_amps}A >= current={current_amps:.0f}A → no action")
            self._pm_throttle_amps = None
            return

        self._pm_throttle_amps = max(safe_amps, 5)  # minimo 5A
        self.log(f"🟡 PM gialla: riduco da {current_amps:.0f}A a {self._pm_throttle_amps}A")

        # Applica subito la riduzione (non aspetta il prossimo ciclo DLM)
        self._set_charging_amps(self._pm_throttle_amps)

        self._send_telegram(
            f"{TG_HEADER} 🟡\n"
            f"⚠️ *Power Manager: zona gialla*\n"
            f"Riduco ricarica: {current_amps:.0f}A → {self._pm_throttle_amps}A\n"
            f"_Protezione distacco attiva_"
        )

    def _pm_on_red(self):
        """PM zona rossa: scendi al minimo immediatamente."""
        current_amps = self._get_float(TESLA_AMPS, default=0)
        min_amp, _ = self._get_amp_limits()

        self._pm_throttle_amps = min_amp  # tipicamente 5A
        self.log(f"🔴 PM ROSSA: riduco da {current_amps:.0f}A a {self._pm_throttle_amps}A!")

        # Applica SUBITO
        self._set_charging_amps(self._pm_throttle_amps)

        self._send_telegram(
            f"{TG_HEADER} 🔴\n"
            f"🚨 *Power Manager: ZONA ROSSA!*\n"
            f"Ricarica al minimo: {current_amps:.0f}A → {self._pm_throttle_amps}A\n"
            f"_Rischio distacco!_"
        )

    def _calc_pm_safe_amps(self):
        """
        Calcola gli ampere massimi per far rientrare la potenza
        sotto la soglia verde del Power Manager.

        Formula: amps_attuali - (potenza_rete - soglia_verde) / voltaggio
        """
        try:
            pm_attrs = self.get_state(PM_ZONE_SENSOR, attribute="all")
            if not pm_attrs or "attributes" not in pm_attrs:
                return 16  # fallback, nessun limite

            attrs = pm_attrs["attributes"]
            grid_power = float(attrs.get("grid_power", 0))
            green_threshold = float(attrs.get("green_threshold", 5000))

            excess = grid_power - green_threshold
            if excess <= 0:
                return 16  # già sotto soglia verde

            voltage = self._get_float(WALLBOX_VOLTAGE, default=230.0)
            current_amps = self._get_float(TESLA_AMPS, default=0)

            # Quanti ampere devo ridurre per eliminare l'eccesso
            amps_to_reduce = int(excess / voltage) + 1  # +1 per sicurezza
            safe_amps = int(current_amps) - amps_to_reduce

            self.log(f"  PM calc: rete={grid_power:.0f}W, verde={green_threshold:.0f}W, "
                     f"eccesso={excess:.0f}W, ridurre={amps_to_reduce}A → safe={safe_amps}A")

            return max(safe_amps, 5)  # minimo 5A

        except Exception as e:
            self.log(f"⚠️ Errore calcolo PM safe amps: {e}", level="WARNING")
            return 5  # in caso di errore, vai al minimo

    # ─────────────────────────────────────────────────────────────────
    # NOTIFICHE TELEGRAM: START / STATUS / STOP
    # ─────────────────────────────────────────────────────────────────

    def _get_mode_emoji(self, mode):
        """Emoji per ogni modalità."""
        return {
            "PV DLM": "☀️",
            "Grid DLM": "🔌",
            "Off Peak DLM": "🌙",
            "Inverter DLM": "🔄",
            "Octopus DLM": "🐙",
        }.get(mode, "⚡")

    def _get_avg_power_kw(self, minutes=30):
        """Calcola la potenza media di ricarica negli ultimi N minuti dalla history HA."""
        try:
            end = datetime.now()
            start = end - timedelta(minutes=minutes)
            history = self.get_history(
                entity_id=TESLA_CHARGE_POWER_KW,
                start_time=start,
                end_time=end
            )
            if not history or not history[0]:
                return None

            values = []
            for entry in history[0]:
                try:
                    val = float(entry.get("state", 0))
                    if val >= 0:  # ignora valori negativi/errati
                        values.append(val)
                except (ValueError, TypeError):
                    pass

            if not values:
                return None

            return round(sum(values) / len(values), 2)

        except Exception as e:
            self.log(f"⚠️ Errore calcolo potenza media: {e}", level="WARNING")
            return None

    def _send_start_notification(self, mode):
        """Messaggio Telegram all'avvio della ricarica."""
        self._stop_notified = False  # reset flag anti-duplicazione
        emoji = self._get_mode_emoji(mode)
        soc = self._get_float(TESLA_BATTERY, default=0)
        target = self._get_float(CHARGE_TARGET, default=80)
        energy = self._get_float(TESLA_ENERGY_ADDED, default=0)
        power_kw = self._get_float(TESLA_CHARGE_POWER_KW, default=0)
        amps = self._get_float(TESLA_AMPS, default=0)
        luna_soc = self._get_float(LUNA_SOC, default=0)

        # Salva riferimenti per il delta kWh
        self._energy_at_start = energy
        self._charge_start_time = datetime.now()

        msg = (
            f"{TG_HEADER} {emoji} *{mode}*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"▶️ *Ricarica avviata*\n"
            f"\n"
            f"🔋 Batteria: *{soc:.0f}%* → Target: *{target:.0f}%*\n"
            f"⚡ Potenza: *{power_kw:.1f} kW* ({amps:.0f}A)\n"
            f"📊 Energy added: *{energy:.2f} kWh*\n"
            f"🏠 Luna2000: *{luna_soc:.0f}%*\n"
            f"\n"
            f"📡 Status report ogni 30 min"
        )
        self._send_telegram(msg)

    def _send_stop_notification(self, mode, reason=""):
        """Messaggio Telegram alla fine della ricarica."""
        emoji = self._get_mode_emoji(mode)
        soc = self._get_float(TESLA_BATTERY, default=0)
        energy_now = self._get_float(TESLA_ENERGY_ADDED, default=0)
        luna_soc = self._get_float(LUNA_SOC, default=0)

        # Calcola delta kWh e durata sessione
        delta_kwh = 0.0
        duration_str = "--:--"
        if self._energy_at_start is not None:
            delta_kwh = energy_now - self._energy_at_start
            if delta_kwh < 0:
                delta_kwh = 0.0  # reset contatore total_increasing
        if self._charge_start_time:
            elapsed = datetime.now() - self._charge_start_time
            hours = int(elapsed.total_seconds() // 3600)
            minutes = int((elapsed.total_seconds() % 3600) // 60)
            duration_str = f"{hours}h {minutes:02d}m"

        msg = (
            f"{TG_HEADER} {emoji} *{mode}*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏹️ *Ricarica terminata*\n"
            f"\n"
            f"🔋 Batteria: *{soc:.0f}%*\n"
            f"⚡ Caricati: *{delta_kwh:.2f} kWh*\n"
            f"⏱️ Durata: *{duration_str}*\n"
            f"🏠 Luna2000: *{luna_soc:.0f}%*\n"
        )
        if reason:
            msg += f"\n📌 _{reason}_"

        self._send_telegram(msg)

        # Reset tracking
        self._energy_at_start = None
        self._charge_start_time = None

    def _send_status_report(self, kwargs=None):
        """Report periodico ogni 30 minuti durante la ricarica."""
        mode = self.get_state(CHARGE_MODE_SELECT)
        if not mode or mode == "Off":
            self._stop_status_reporting()
            return

        emoji = self._get_mode_emoji(mode)
        soc = self._get_float(TESLA_BATTERY, default=0)
        target = self._get_float(CHARGE_TARGET, default=80)
        energy_now = self._get_float(TESLA_ENERGY_ADDED, default=0)
        power_kw = self._get_float(TESLA_CHARGE_POWER_KW, default=0)
        amps = self._get_float(TESLA_AMPS, default=0)
        luna_soc = self._get_float(LUNA_SOC, default=0)
        voltage = self._get_float(WALLBOX_VOLTAGE, default=230)

        # Potenza media ultimi 30 min
        avg_power = self._get_avg_power_kw(minutes=30)
        if avg_power is not None:
            power_str = f"⚡ Potenza media (30m): *{avg_power:.1f} kW* (ora: {power_kw:.1f} kW)"
        else:
            power_str = f"⚡ Potenza: *{power_kw:.1f} kW* ({amps:.0f}A × {voltage:.0f}V)"

        # Delta kWh dalla partenza
        delta_kwh = 0.0
        duration_str = "--:--"
        if self._energy_at_start is not None:
            delta_kwh = energy_now - self._energy_at_start
            if delta_kwh < 0:
                delta_kwh = 0.0
        if self._charge_start_time:
            elapsed = datetime.now() - self._charge_start_time
            hours = int(elapsed.total_seconds() // 3600)
            minutes = int((elapsed.total_seconds() % 3600) // 60)
            duration_str = f"{hours}h {minutes:02d}m"

        # Barra progresso testuale
        progress = min(100, max(0, soc))
        bar_len = 10
        filled = int(bar_len * progress / 100)
        bar = "█" * filled + "░" * (bar_len - filled)

        msg = (
            f"{TG_HEADER} {emoji} *{mode}*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 *Status Report*\n"
            f"\n"
            f"🔋 [{bar}] *{soc:.0f}%* / {target:.0f}%\n"
            f"{power_str}\n"
            f"📈 Sessione: *+{delta_kwh:.2f} kWh* in {duration_str}\n"
            f"📊 Energy added totale: *{energy_now:.2f} kWh*\n"
            f"🏠 Luna2000: *{luna_soc:.0f}%*\n"
        )

        # Info aggiuntive per Off Peak: countdown
        if mode == "Off Peak DLM":
            countdown = self.get_state("sensor.charge_countdown")
            if countdown and countdown not in ("unknown", "unavailable"):
                msg += f"⏳ Countdown F3: *{countdown}*\n"

        # Info aggiuntive per Octopus: stato dispatching
        if mode == "Octopus DLM":
            octo = self.get_state(OCTOPUS_DISPATCHING)
            msg += f"🐙 Dispatching: *{'Attivo' if octo == 'on' else 'Spento'}*\n"

        # Info Power Manager se throttling attivo
        if self._pm_throttle_amps is not None:
            pm_zone = self.get_state(PM_ZONE_SENSOR) or "?"
            zone_emoji = {"green": "🟢", "yellow": "🟡", "red": "🔴"}.get(pm_zone, "⚪")
            msg += f"{zone_emoji} PM: cap *{self._pm_throttle_amps}A* (zona {pm_zone})\n"

        self._send_telegram(msg)

    def _start_status_reporting(self):
        """Avvia il report periodico ogni 30 minuti."""
        self._stop_status_reporting()
        self._status_report_handle = self.run_every(
            self._send_status_report,
            f"now+{STATUS_REPORT_INTERVAL}",
            STATUS_REPORT_INTERVAL
        )
        self.log(f"📡 Status report attivato (ogni {STATUS_REPORT_INTERVAL // 60} min)")

    def _stop_status_reporting(self):
        """Ferma il report periodico."""
        if self._status_report_handle:
            self.cancel_timer(self._status_report_handle)
            self._status_report_handle = None

    # =====================================================================
    # STOP RICARICA
    # =====================================================================

    def _stop_charging(self, reason=""):
        """Ferma la ricarica e resetta tutto."""
        self.log(f"🛑 STOP ricarica: {reason}")

        # Invia notifica stop e setta flag per evitare duplicati
        # (_set_charge_mode("Off") triggera _on_mode_changed che
        # NON deve inviare un'altra notifica stop)
        current_mode = self.get_state(CHARGE_MODE_SELECT)
        if current_mode and current_mode != "Off":
            self._send_stop_notification(current_mode, reason)
            self._stop_notified = True  # flag anti-duplicazione

        # Ferma il report periodico
        self._stop_status_reporting()

        # Cancella il loop DLM attivo
        self._cancel_dlm_loop()

        # Cancella countdown off-peak
        if self._offpeak_countdown_handle:
            self.cancel_timer(self._offpeak_countdown_handle)
            self._offpeak_countdown_handle = None
        if self._offpeak_trigger_handle:
            self.cancel_timer(self._offpeak_trigger_handle)
            self._offpeak_trigger_handle = None

        # Spegni charger
        self._charger_off()

        # Ripristina scarica Luna2000
        self._set_luna_discharge(LUNA_DISCHARGE_FULL)

        # Rilascia limite Power Manager
        self._pm_throttle_amps = None

        # Imposta modalità Off
        self._set_charge_mode("Off")

    def _cancel_dlm_loop(self):
        """Cancella il loop DLM corrente."""
        if self._dlm_handle:
            self.cancel_timer(self._dlm_handle)
            self._dlm_handle = None

    # =====================================================================
    # SEQUENZA DI AVVIO RICARICA
    # =====================================================================

    def _on_mode_changed(self, entity, attribute, old, new, kwargs):
        """
        Callback quando cambia input_select.tesla_chargemode_select.
        Gestisce sia l'attivazione (Off → modalità) che lo spegnimento.
        """
        self.log(f"🔄 Modalità cambiata: {old} → {new}")

        # Cancella auto-start Grid DLM se utente ha scelto (da dashboard)
        # e aggiorna il messaggio Telegram se c'era una scelta pendente
        if self._tg_choice_message_id and new != "Off":
            emoji = self._get_mode_emoji(new)
            self._edit_telegram_choice_message(
                f"{TG_HEADER}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"{emoji} *{new}* selezionato da dashboard\n\n"
                f"Avvio sequenza di ricarica..."
            )
        elif self._tg_choice_message_id and new == "Off":
            self._edit_telegram_choice_message(
                f"{TG_HEADER}\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"❌ *Annullato da dashboard*"
            )
        self._cancel_auto_grid()

        # ── Spegnimento: qualsiasi → Off ──
        if new == "Off":
            if old and old != "Off":
                if not self._stop_notified:
                    self._send_stop_notification(old, "Spento manualmente")
                self._stop_notified = False  # reset flag
                self._stop_status_reporting()
                self._cancel_dlm_loop()
                self._charger_off()
                self._set_luna_discharge(LUNA_DISCHARGE_FULL)
                # Valuta se spegnere polling (notte + casa + no ricarica)
                self._evaluate_polling_after_charge_stop()
            return

        # ── Cambio modalità in corso: cancella loop precedente ──
        if old and old != "Off":
            if not self._stop_notified:
                self._send_stop_notification(old, f"Cambio a {new}")
            self._stop_notified = False  # reset flag
            self._stop_status_reporting()
        self._cancel_dlm_loop()

        # ── Attivazione: Off → modalità ──
        if old == "Off":
            self._start_charge_sequence(new)
        else:
            # Cambio diretto tra modalità attive → avvia il nuovo loop
            self._start_dlm_loop(new)

    def _start_charge_sequence(self, mode):
        """
        Sequenza di avvio ricarica:
        1. Set amp 0 → polling on
        2. 5s → wake up (force data update)
        3. 15s → verifica freshness dati
        4. Verifica Tesla a casa (con dati aggiornati!)
        5. Accendi charger
        6. Check/set target 100%
        7. Avvia loop DLM
        """
        if self._startup_running:
            self.log("⚠️ Sequenza di avvio già in corso, ignoro")
            return

        self._startup_running = True

        # Cancella eventuale auto-start Grid DLM pendente
        self._cancel_auto_grid()

        # 1. Preparazione
        self._set_charging_amps(0)
        self._polling_on("avvio ricarica")

        # 2. Dopo 5s attiva wake up (polling ha bisogno di tempo per attivarsi)
        self.run_in(
            self._wake_then_continue,
            5,
            mode=mode,
            next_step="_start_charge_sequence_part2"
        )

    def _wake_then_continue(self, kwargs):
        """
        Passo intermedio: preme Force Data Update e dopo 15s
        continua con la callback indicata (part2 o octopus_part2).
        """
        mode = kwargs.get("mode", "Off")
        next_step_name = kwargs.get("next_step", "_start_charge_sequence_part2")

        # Verifica che la modalità sia ancora quella richiesta
        if next_step_name == "_start_charge_sequence_part2" and not self._is_mode_active(mode):
            self.log("⚠️ Modalità cambiata durante wake → annullo")
            self._startup_running = False
            return

        self._force_wake_up()
        self.log("🔄 Wake up inviato, attendo 15s per aggiornamento sensori...")

        # Dopo 15s continua con la callback appropriata
        if next_step_name == "_octopus_start_part2":
            self.run_in(self._octopus_start_part2, 15)
        else:
            self.run_in(
                self._start_charge_sequence_part2,
                15,
                mode=mode
            )

    def _start_charge_sequence_part2(self, kwargs):
        """Seconda parte della sequenza di avvio (dopo wake up + 15s)."""
        mode = kwargs.get("mode", "Off")

        # Verifica che la modalità sia ancora quella richiesta
        if not self._is_mode_active(mode):
            self.log(f"⚠️ Modalità cambiata durante avvio → annullo")
            self._startup_running = False
            return

        # ── Verifica freshness dati Tesla ──
        if not self._is_data_fresh():
            self.log("⚠️ Dati Tesla non aggiornati dopo wake up → annullo")
            self._send_telegram(
                f"{TG_HEADER}\n"
                f"⚠️ *Avvio annullato*\n"
                f"I dati Tesla non si sono aggiornati dopo il wake up.\n"
                f"Riprova o verifica la connettività."
            )
            self._set_charge_mode("Off")
            self._startup_running = False
            return

        # ── Verifica Tesla a casa (con dati AGGIORNATI) ──
        location = self.get_state(TESLA_LOCATION)
        if location != "home":
            self.log(f"🏠 Tesla non a casa (stato: {location}) → annullo")
            self._send_telegram(
                f"{TG_HEADER}\n"
                f"🚗❌ *Tesla non a casa!*\n"
                f"Posizione: _{location}_\n"
                f"Modalità *{mode}* annullata."
            )
            self._set_charge_mode("Off")
            self._startup_running = False
            return

        # 4. Accendi charger
        self._charger_on()

        # 5. Check 100% settimanale e set target
        self._update_weekly_100_sensor()

        # 6. Avvia il loop DLM dopo un breve delay (per dare tempo al charger)
        self.run_in(
            lambda kwargs: self._start_dlm_loop(kwargs.get("mode")),
            5,
            mode=mode
        )
        self._startup_running = False

    # =====================================================================
    # LOOP DLM PRINCIPALE
    # =====================================================================

    def _start_dlm_loop(self, mode):
        """Avvia il loop DLM appropriato per la modalità."""
        self._cancel_dlm_loop()

        # PV DLM: spegni Octopus Smart EV (ha prevalenza sulla ricarica PV)
        if mode == "PV DLM":
            octopus_ev = self.get_state(OCTOPUS_SMART_EV)
            if octopus_ev == "on":
                self.log("☀️ PV DLM → spengo Octopus Smart EV (avrebbe prevalenza)")
                self.call_service("switch/turn_off", entity_id=OCTOPUS_SMART_EV)
                self._send_telegram(
                    f"{TG_HEADER} ☀️\n"
                    f"🐙→❌ Octopus Smart EV *spento*\n"
                    f"_Disattivato per permettere ricarica PV_"
                )

        # Notifica Telegram e avvia report periodico
        self._send_start_notification(mode)
        self._start_status_reporting()

        if mode == "PV DLM":
            self._dlm_cycle_pv({})
        elif mode == "Grid DLM":
            self._dlm_cycle_grid({})
        elif mode == "Off Peak DLM":
            self._start_offpeak(mode)
        elif mode == "Inverter DLM":
            self._dlm_cycle_inverter({})
        # Octopus DLM è gestito dal suo listener dedicato
        else:
            self.log(f"⚠️ Modalità sconosciuta: {mode}")

    # ─────────────────────────────────────────────────────────────────
    # PV DLM
    # ─────────────────────────────────────────────────────────────────

    def _dlm_cycle_pv(self, kwargs):
        """
        Ciclo DLM PV: calcola surplus → setta ampere → attendi → check.
        Usa _calc_pv_surplus() che include +wallbox (riallocabile).
        """
        mode = "PV DLM"
        if not self._is_mode_active(mode):
            return

        if not self._should_continue_charging(mode):
            self._stop_charging("Target raggiunto (PV DLM)")
            return

        if not self._is_charger_on():
            self._stop_charging("Charger spento (PV DLM)")
            return

        # Calcola surplus e imposta ampere
        surplus = self._calc_pv_surplus()
        amps = self._watts_to_amps(surplus)
        self.log(f"  ☀️ PV DLM: surplus={surplus:.0f}W → {amps}A")
        self._set_charging_amps(amps)

        # Check dopo 30s
        self._dlm_handle = self.run_in(self._dlm_check_pv, 30)

    def _dlm_check_pv(self, kwargs):
        """Check PV: verifica se serve ricalcolare."""
        mode = "PV DLM"
        if not self._is_mode_active(mode):
            return

        if not self._should_continue_charging(mode):
            self._stop_charging("Target raggiunto (PV DLM check)")
            return

        if not self._is_charger_on():
            self._stop_charging("Charger spento (PV DLM check)")
            return

        if self._check_needs_adjustment_pv():
            # Ricalcola subito con la formula completa (+wallbox)
            self.log("  ☀️ PV check: ricalcolo necessario")
            self._dlm_cycle_pv({})
        else:
            # Stabile, ricontrolla tra 30s
            self._dlm_handle = self.run_in(self._dlm_check_pv, 30)

    # ─────────────────────────────────────────────────────────────────
    # GRID DLM
    # ─────────────────────────────────────────────────────────────────

    def _dlm_cycle_grid(self, kwargs):
        """
        Ciclo DLM Grid: calcola potenza disponibile → setta ampere.
        
        Formula: available = meter + grid_active + wallbox
        
        Il wallbox viene AGGIUNTO perché rappresenta potenza già allocata
        a Tesla che può essere riallocata al nuovo calcolo.
        """
        mode = "Grid DLM"
        if not self._is_mode_active(mode):
            return

        if not self._should_continue_charging(mode):
            self._set_luna_discharge(LUNA_DISCHARGE_FULL)
            self._stop_charging("Target raggiunto (Grid DLM)")
            return

        if not self._is_charger_on():
            self._set_luna_discharge(LUNA_DISCHARGE_FULL)
            self._stop_charging("Charger spento (Grid DLM)")
            return

        # Blocca scarica Luna2000 durante DLM Grid
        self._set_luna_discharge(LUNA_DISCHARGE_OFF)

        # Calcola con formula: meter + grid + wallbox
        available = self._calc_grid_available()
        amps = self._watts_to_amps(available)
        self.log(f"  🔌 Grid DLM: available={available:.0f}W → {amps}A")
        self._set_charging_amps(amps)

        # Check dopo 30s
        self._dlm_handle = self.run_in(self._dlm_check_grid, 30)

    def _dlm_check_grid(self, kwargs):
        """
        Controlla se serve ricalcolare usando headroom = meter + grid (senza wallbox).
        """
        mode = "Grid DLM"
        if not self._is_mode_active(mode):
            return

        if not self._should_continue_charging(mode):
            self._set_luna_discharge(LUNA_DISCHARGE_FULL)
            self._stop_charging("Target raggiunto (Grid DLM check)")
            return

        if not self._is_charger_on():
            self._set_luna_discharge(LUNA_DISCHARGE_FULL)
            self._stop_charging("Charger spento (Grid DLM check)")
            return

        if self._check_needs_adjustment_grid():
            # Ricalcola con formula completa (meter + grid + wallbox)
            self.log("  🔌 Grid check: ricalcolo necessario")
            self._dlm_cycle_grid({})
        else:
            # Stabile, ricontrolla tra 30s
            self._dlm_handle = self.run_in(self._dlm_check_grid, 30)

    # ─────────────────────────────────────────────────────────────────
    # INVERTER DLM
    # ─────────────────────────────────────────────────────────────────

    def _dlm_cycle_inverter(self, kwargs):
        """
        Ciclo DLM Inverter: calcola potenza disponibile → setta ampere.
        Formula: inverter_max + grid_active - active_power + wallbox
        """
        mode = "Inverter DLM"
        if not self._is_mode_active(mode):
            return

        luna_soc = self._get_float(LUNA_SOC, default=0)
        if luna_soc <= 10:
            self._stop_charging("Luna2000 SOC ≤ 10% (Inverter DLM)")
            return

        if not self._should_continue_charging(mode):
            self._stop_charging("Target raggiunto (Inverter DLM)")
            return

        if not self._is_charger_on():
            self._stop_charging("Charger spento (Inverter DLM)")
            return

        available = self._calc_inverter_available()
        amps = self._watts_to_amps(available)
        self.log(f"  🔄 Inv DLM: available={available:.0f}W → {amps}A")
        self._set_charging_amps(amps)

        self._dlm_handle = self.run_in(self._dlm_check_inverter, 30)

    def _dlm_check_inverter(self, kwargs):
        """Check Inverter: usa headroom senza wallbox per decidere."""
        mode = "Inverter DLM"
        if not self._is_mode_active(mode):
            return

        luna_soc = self._get_float(LUNA_SOC, default=0)
        if luna_soc <= 10:
            self._stop_charging("Luna2000 SOC ≤ 10% (Inverter DLM check)")
            return

        if not self._should_continue_charging(mode):
            self._stop_charging("Target raggiunto (Inverter DLM check)")
            return

        if not self._is_charger_on():
            self._stop_charging("Charger spento (Inverter DLM check)")
            return

        if self._check_needs_adjustment_inverter():
            self.log("  🔄 Inv check: ricalcolo necessario")
            self._dlm_cycle_inverter({})
        else:
            self._dlm_handle = self.run_in(self._dlm_check_inverter, 30)

    # ─────────────────────────────────────────────────────────────────
    # OFF PEAK DLM
    # ─────────────────────────────────────────────────────────────────

    def _start_offpeak(self, mode):
        """
        Avvia la modalità Off Peak DLM.
        Setta target 100% e attende la fascia F3 (23-07 o festivo).
        """
        # Imposta target 100%
        self._set_charge_target(100)

        # Verifica se è off-peak adesso
        if self._is_offpeak_now():
            self.log("🌙 Off Peak: fascia F3 attiva → avvio DLM Grid")
            self._dlm_cycle_offpeak({})
        else:
            # Calcola tempo fino alle 23:00
            now = datetime.now()
            target_23 = now.replace(hour=23, minute=0, second=0, microsecond=0)
            if now >= target_23:
                # Già passate le 23, aspetta domani (non dovrebbe succedere)
                target_23 += timedelta(days=1)

            wait_seconds = (target_23 - now).total_seconds()
            hours = int(wait_seconds // 3600)
            minutes = int((wait_seconds % 3600) // 60)

            self.log(f"🌙 Off Peak: attesa {hours}h {minutes}m fino alle 23:00")

            # Aggiorna sensore countdown
            self._update_countdown_sensor(wait_seconds)

            # Programma avvio alle 23:00
            self._offpeak_trigger_handle = self.run_in(
                self._offpeak_trigger,
                int(wait_seconds)
            )

            # Aggiorna countdown ogni 60 secondi
            self._offpeak_countdown_handle = self.run_every(
                self._offpeak_countdown_tick,
                "now",
                60  # ogni minuto
            )

    def _is_offpeak_now(self):
        """Verifica se siamo in fascia off-peak (F3 o festivo)."""
        is_working = self.get_state(WORKING_DAY) == "on"

        if not is_working:
            return True  # Festivo → sempre off-peak

        hour = datetime.now().hour
        return hour >= 23 or hour < 7

    def _offpeak_trigger(self, kwargs):
        """Triggerato quando inizia la fascia F3."""
        if self._offpeak_countdown_handle:
            self.cancel_timer(self._offpeak_countdown_handle)
            self._offpeak_countdown_handle = None

        # Aggiorna countdown a 00:00:00
        self._set_sensor_state("sensor.charge_countdown", "00:00:00",
                               attributes={"countdown": "00:00:00"})

        self.log("🌙 Off Peak: fascia F3 iniziata → avvio DLM Grid")
        self._dlm_cycle_offpeak({})

    def _offpeak_countdown_tick(self, kwargs):
        """Aggiorna il sensore countdown."""
        now = datetime.now()
        target_23 = now.replace(hour=23, minute=0, second=0, microsecond=0)
        if now >= target_23:
            remaining = 0
        else:
            remaining = (target_23 - now).total_seconds()
        self._update_countdown_sensor(remaining)

    def _update_countdown_sensor(self, remaining_seconds):
        """Aggiorna il sensore Charge Countdown."""
        hours = int(remaining_seconds // 3600)
        minutes = int((remaining_seconds % 3600) // 60)
        seconds = int(remaining_seconds % 60)
        formatted = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        self._set_sensor_state("sensor.charge_countdown", formatted,
                               attributes={"countdown": formatted})

    def _dlm_cycle_offpeak(self, kwargs):
        """Ciclo DLM Off Peak (usa logica Grid: meter + grid + wallbox)."""
        mode = "Off Peak DLM"
        if not self._is_mode_active(mode):
            return

        tariff = self.get_state(TARIFF_BAND)
        if tariff != "F3" and self.get_state(WORKING_DAY) == "on":
            self._stop_charging("Fine fascia F3 (Off Peak DLM)")
            return

        if not self._should_continue_charging(mode):
            self._stop_charging("Target raggiunto (Off Peak DLM)")
            return

        if not self._is_charger_on():
            self._stop_charging("Charger spento (Off Peak DLM)")
            return

        # Stessa formula Grid: meter + grid + wallbox
        available = self._calc_grid_available()
        amps = self._watts_to_amps(available)
        self.log(f"  🌙 OffPeak DLM: available={available:.0f}W → {amps}A")
        self._set_charging_amps(amps)

        self._dlm_handle = self.run_in(self._dlm_check_offpeak, 35)

    def _dlm_check_offpeak(self, kwargs):
        """Check Off Peak: usa check Grid (headroom senza wallbox)."""
        mode = "Off Peak DLM"
        if not self._is_mode_active(mode):
            return

        tariff = self.get_state(TARIFF_BAND)
        if tariff != "F3" and self.get_state(WORKING_DAY) == "on":
            self._stop_charging("Fine fascia F3 (Off Peak check)")
            return

        if not self._should_continue_charging(mode):
            self._stop_charging("Target raggiunto (Off Peak check)")
            return

        if not self._is_charger_on():
            self._stop_charging("Charger spento (Off Peak check)")
            return

        if self._check_needs_adjustment_grid():
            self.log("  🌙 OffPeak check: ricalcolo necessario")
            self._dlm_cycle_offpeak({})
        else:
            self._dlm_handle = self.run_in(self._dlm_check_offpeak, 35)

    # ─────────────────────────────────────────────────────────────────
    # OCTOPUS DLM
    # ─────────────────────────────────────────────────────────────────

    def _on_octopus_dispatching(self, entity, attribute, old, new, kwargs):
        """
        Triggerato quando Octopus Intelligent Dispatching diventa ON.
        Sequenza: amp 0 → polling on → wake up → delay → charger on →
                  check 100% → set target → Luna off → avvia DLM Grid.
        """
        self.log("🐙 Octopus Intelligent Dispatching attivato!")

        # Cancella eventuali loop precedenti
        self._cancel_dlm_loop()

        # Sequenza avvio Octopus
        self._set_charging_amps(0)
        self._polling_on("avvio Octopus")

        # Dopo 5s attiva wake up, poi dopo 15s continua
        self.run_in(
            self._wake_then_continue,
            5,
            mode="Octopus DLM",
            next_step="_octopus_start_part2"
        )

    def _octopus_start_part2(self, kwargs):
        """Seconda parte avvio Octopus."""

        # ── Verifica freshness dati Tesla ──
        if not self._is_data_fresh():
            self.log("⚠️ Dati Tesla non aggiornati dopo wake up (Octopus) → annullo")
            self._send_telegram(
                f"{TG_HEADER} 🐙\n"
                f"⚠️ *Avvio Octopus annullato*\n"
                f"I dati Tesla non si sono aggiornati dopo il wake up."
            )
            return

        # ── Verifica Tesla a casa (con dati AGGIORNATI) ──
        location = self.get_state(TESLA_LOCATION)
        if location != "home":
            self.log(f"🏠 Tesla non a casa (stato: {location}) → Octopus annullato")
            self._send_telegram(
                f"{TG_HEADER} 🐙\n"
                f"🚗❌ *Tesla non a casa!*\n"
                f"Posizione: _{location}_\n"
                f"Octopus DLM annullato."
            )
            return

        # Accendi charger
        self._charger_on()

        # Check e set target 100% settimanale
        weekly_status = self._check_weekly_100()
        if weekly_status == "Must charge 100%":
            self._set_charge_target(100)
        else:
            self._set_charge_target(80)

        # Aggiorna sensore 100% settimanale
        self._update_weekly_100_sensor()

        # Blocca scarica Luna2000
        self._set_luna_discharge(LUNA_DISCHARGE_OFF)

        # Notifica Telegram e avvia report periodico
        self._send_start_notification("Octopus DLM")
        self._start_status_reporting()

        # Avvia loop DLM Grid con check Octopus
        self.run_in(self._dlm_cycle_octopus, 5)

    def _dlm_cycle_octopus(self, kwargs):
        """Ciclo DLM Octopus (usa logica Grid con check dispatching)."""
        if self.get_state(OCTOPUS_DISPATCHING) != "on":
            self.log("🐙 Octopus dispatching terminato → stop")
            self._set_luna_discharge(LUNA_DISCHARGE_FULL)
            self._stop_charging("Octopus dispatching OFF")
            return

        if not self._is_charger_on():
            self.log("🐙 Charger spento durante Octopus → stop")
            self._set_luna_discharge(LUNA_DISCHARGE_FULL)
            self._stop_charging("Charger spento (Octopus)")
            return

        # Stessa formula Grid: meter + grid + wallbox
        available = self._calc_grid_available()
        amps = self._watts_to_amps(available)
        self.log(f"  🐙 Octopus DLM: available={available:.0f}W → {amps}A")
        self._set_charging_amps(amps)

        self._dlm_handle = self.run_in(self._dlm_check_octopus, 15)

    def _dlm_check_octopus(self, kwargs):
        """Check Octopus: usa check Grid (headroom senza wallbox)."""
        if self.get_state(OCTOPUS_DISPATCHING) != "on":
            self._set_luna_discharge(LUNA_DISCHARGE_FULL)
            self._stop_charging("Octopus dispatching OFF (check)")
            return

        if not self._is_charger_on():
            self._set_luna_discharge(LUNA_DISCHARGE_FULL)
            self._stop_charging("Charger spento (Octopus check)")
            return

        if self._check_needs_adjustment_grid():
            self.log("  🐙 Octopus check: ricalcolo necessario")
            self._dlm_cycle_octopus({})
        else:
            self._dlm_handle = self.run_in(self._dlm_check_octopus, 15)

    # =====================================================================
    # TRACKING RICARICA 100% SETTIMANALE
    # =====================================================================

    def _check_weekly_100(self):
        """
        Verifica se la Tesla è stata caricata al 100% negli ultimi 7 giorni.
        Usa l'history di HA.
        Returns: "100% charge ok" o "Must charge 100%"
        """
        try:
            end = datetime.now()
            start = end - timedelta(days=14)  # 2 settimane di history

            history = self.get_history(
                entity_id=TESLA_BATTERY,
                start_time=start,
                end_time=end
            )

            if not history or not history[0]:
                return "Must charge 100%"

            seven_days_ago = end - timedelta(days=7)
            count = 0

            for entry in history[0]:
                if entry.get("state") == "100":
                    last_changed = datetime.fromisoformat(
                        entry["last_changed"].replace("Z", "+00:00")
                    )
                    if last_changed.replace(tzinfo=None) >= seven_days_ago:
                        count += 1

            return "100% charge ok" if count > 0 else "Must charge 100%"

        except Exception as e:
            self.log(f"⚠️ Errore check 100%: {e}", level="WARNING")
            return "Must charge 100%"

    def _update_weekly_100_sensor(self):
        """
        Aggiorna il sensore 'Tesla 100% 1w' con i dati della ricarica settimanale.
        Persistenza: salva la data dell'ultima carica 100% in input_text.tesla_last_100_date
        così sopravvive ai riavvii e alla finestra limitata della history HA.
        """
        try:
            end = datetime.now()
            start = end - timedelta(days=14)

            history = self.get_history(
                entity_id=TESLA_BATTERY,
                start_time=start,
                end_time=end
            )

            if not history or not history[0]:
                # Nessuna history — usa dato persistente come fallback
                last_charge_all_time = self._read_persistent_last_100()
                days_ago = self._calc_days_ago(last_charge_all_time)
                self._set_sensor_state("sensor.tesla_100_1w", "Must charge 100%",
                    attributes={
                        "date_time": None,
                        "count": "00",
                        "weekly_charge_status": "Must charge 100%",
                        "days_remaining": 7,
                        "last_charge_all_time": last_charge_all_time,
                        "days_ago": days_ago
                    })
                self.log(f"📊 100% tracking: Must charge 100%, "
                         f"count=0, days_remaining=7, last={last_charge_all_time}")
                self._set_charge_target(100)
                return

            seven_days_ago = end - timedelta(days=7)
            full_charges_7d = []
            last_full_ever = None

            for entry in history[0]:
                if entry.get("state") == "100":
                    last_full_ever = entry
                    try:
                        event_date = datetime.fromisoformat(
                            entry["last_changed"].replace("Z", "+00:00")
                        ).replace(tzinfo=None)
                        if event_date >= seven_days_ago:
                            full_charges_7d.append(entry)
                    except (ValueError, KeyError):
                        pass

            count = len(full_charges_7d)
            charge_status = "100% charge ok" if count > 0 else "Must charge 100%"
            days_remaining = 7
            date_time = None

            if count > 0:
                last_event = full_charges_7d[-1]
                date_time = last_event.get("last_changed")
                try:
                    last_date = datetime.fromisoformat(
                        date_time.replace("Z", "+00:00")
                    ).replace(tzinfo=None)
                    diff_days = (end - last_date).days
                    days_remaining = max(7 - diff_days, 0)
                except (ValueError, TypeError):
                    pass

            # Determina last_charge_all_time dalla history
            last_charge_all_time = None
            if last_full_ever:
                try:
                    lc = datetime.fromisoformat(
                        last_full_ever["last_changed"].replace("Z", "+00:00")
                    ).replace(tzinfo=None)
                    last_charge_all_time = lc.strftime("%d/%m/%Y")
                    # Salva in helper persistente
                    self._save_persistent_last_100(last_charge_all_time)
                except (ValueError, KeyError):
                    pass

            # Se history non ha trovato nulla, usa il dato persistente
            if last_charge_all_time is None:
                last_charge_all_time = self._read_persistent_last_100()

            days_ago = self._calc_days_ago(last_charge_all_time)

            self._set_sensor_state("sensor.tesla_100_1w", charge_status,
                attributes={
                    "date_time": date_time,
                    "count": f"{count:02d}",
                    "weekly_charge_status": charge_status,
                    "days_remaining": days_remaining,
                    "last_charge_all_time": last_charge_all_time,
                    "days_ago": days_ago
                })

            self.log(f"📊 100% tracking: {charge_status}, "
                     f"count={count}, days_remaining={days_remaining}")

            # Imposta target in base allo stato settimanale
            if charge_status == "Must charge 100%":
                self._set_charge_target(100)
            else:
                self._set_charge_target(80)

        except Exception as e:
            self.log(f"⚠️ Errore update 100% sensor: {e}", level="WARNING")

    def _save_persistent_last_100(self, date_str):
        """Salva la data dell'ultima carica 100% nell'helper persistente."""
        try:
            self.call_service(
                "input_text/set_value",
                entity_id=LAST_100_HELPER,
                value=date_str
            )
        except Exception as e:
            self.log(f"⚠️ Errore salvataggio {LAST_100_HELPER}: {e}", level="WARNING")

    def _read_persistent_last_100(self):
        """Legge la data dell'ultima carica 100% dall'helper persistente."""
        try:
            val = self.get_state(LAST_100_HELPER)
            if val and val not in ("unknown", "unavailable", ""):
                return val
        except Exception:
            pass
        return None

    def _calc_days_ago(self, date_str):
        """Calcola quanti giorni fa dalla data dd/mm/yyyy. Ritorna None se non disponibile."""
        if not date_str or date_str == "None":
            return None
        try:
            parts = date_str.split("/")
            last_dt = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
            return (datetime.now() - last_dt).days
        except (ValueError, IndexError):
            return None

    def _set_sensor_state(self, entity_id, state, attributes=None):
        """Imposta lo stato di un sensore HA."""
        try:
            self.set_state(entity_id, state=state, attributes=attributes or {})
        except Exception as e:
            self.log(f"⚠️ Errore set_state {entity_id}: {e}", level="WARNING")

    # =====================================================================
    # STARTUP CHECK
    # =====================================================================

    def _on_startup_check(self, kwargs):
        """
        Al riavvio di AppDaemon: aggiorna il sensore 100% settimanale.
        Verifica che HA sia avviato da meno di 5 minuti.
        """
        try:
            uptime_str = self.get_state(HA_UPTIME)
            if uptime_str:
                uptime_dt = datetime.fromisoformat(uptime_str.replace("Z", "+00:00"))
                diff = (datetime.now(uptime_dt.tzinfo) - uptime_dt).total_seconds()
                if diff > 300:  # più di 5 minuti
                    self.log("⏰ HA attivo da >5min, skip startup check rapido")
                    # Aggiorna comunque il sensore 100%
                    self._update_weekly_100_sensor()
                    return

            self._update_weekly_100_sensor()
            self.log("✅ Startup check completato")

        except Exception as e:
            self.log(f"⚠️ Errore startup check: {e}", level="WARNING")
            # Prova comunque ad aggiornare il sensore
            self._update_weekly_100_sensor()
