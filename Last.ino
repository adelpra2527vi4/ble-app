#include <BLEDevice.h>
#include <BLEUtils.h>
#include <BLEServer.h>
#include <BLE2902.h>
#include <Preferences.h>
#include <Adafruit_NeoPixel.h>
#include <initializer_list>

// --- CONFIGURAZIONE SISTEMA ---
// --- SENSORI BLE ---
#define MAX_SENSORS  10
// --- RELE' ---
#define NUM_RELAYS 6
const int relayPins[NUM_RELAYS] = {1, 2, 41, 42, 45, 46};

// --- PINOUT WAVESHARE ESP32-S3-RELAY-6CH ---
#define PIN_NEOPIXEL 38
#define NUM_PIXELS   1
#define BUTTON_BOOT  0
#define LED_BRIGHTNESS 50

// --- SERIALE ESTERNA VERSO MODEM / LORA ---
#define EXT_SERIAL_TX 15
#define EXT_SERIAL_RX 16
#define EXT_SERIAL_BAUD 115200

// --- PARAMETRI SENSORI ELA ---
// --- ELA Innovation Company ID ---
#define ELA_CIN_LSB 0x57
#define ELA_CIN_MSB 0x07

// --- ELA DATA_ID ---
#define ELA_ID_RHT  0x21
#define ELA_ID_TEMP 0x12
#define ELA_ID_PIR  0x92
#define ELA_ID_MOV  0x42
#define ELA_ID_MAG  0x32
#define ELA_ID_DI   0x62
#define ELA_ID_ANG  0x56

// --- UUID Standard BT SIG ---
#define BT_UUID_TEMPERATURE ((uint16_t)0x2A6E)
#define BT_UUID_HUMIDITY    ((uint16_t)0x2A6F)
#define BT_UUID_ALERT_LEVEL ((uint16_t)0x2A06)
#define BT_UUID_RAINFALL    ((uint16_t)0x2A78)
#define BT_UUID_MAG3D       ((uint16_t)0x2AA1)

// --- UUID SERVER ---
#define CONFIG_SERVICE_UUID "DDDD0001-0000-1000-8000-00805F9B34FB"
#define CONFIG_NAME_UUID    "DDDD0001-0000-1000-8000-00805F9BFFFF"
#define CONFIG_COUNT_UUID   "DDDD0001-0000-1000-8000-00805F9BFFFA"
#define CONFIG_RESET_UUID   "DDDD0001-0000-1000-8000-00805F9BFFFE"
#define HUB_SERVICE_UUID    "EEEE0001-0000-1000-8000-00805F9B34FB"
#define RELAY_SERVICE_UUID  "AAAA0001-0000-1000-8000-00805F9B34FB"
#define CONFIG_SNIFFER_UUID "DDDD0001-0000-1000-8000-00805F9BAAAA"


// --- TEMPISTICHE ---
// --- LED ---
#define FLASH_DURATION  150
#define INTER_FLASH_GAP 150
#define END_CYCLE_PAUSE 1500

// --- SENSOR TIMEOUT ---
#define SENSOR_TIMEOUT_MS 60000UL

// --- MQTT KEEPALIVE (secondi) ---
#define MQTT_KEEPALIVE 300
#define MQTT_KEEPALIVE_STR "300"

// ================================================================
//  GLOBALI
// ================================================================
Adafruit_NeoPixel pixels(NUM_PIXELS, PIN_NEOPIXEL, NEO_RGB + NEO_KHZ800);
Preferences preferences;

SemaphoreHandle_t serialMutex;
SemaphoreHandle_t dataMutex;

TaskHandle_t bleTaskHandle  = NULL;
TaskHandle_t mqttTaskHandle = NULL;

String targetMacAddresses[MAX_SENSORS];
String targetRules[MAX_SENSORS];
volatile boolean connected[MAX_SENSORS] = {0};
BLECharacteristic* pHubChars[MAX_SENSORS] = {NULL};
String hubName = "Hub-NotConfigurated";

String sensorDataBuffer[MAX_SENSORS];
volatile bool sensorDataReady[MAX_SENSORS] = {false};
unsigned long lastSeenTime[MAX_SENSORS] = {0};

int currentMode = 0;
volatile int activeSensors = 4;
bool deviceConnectedToMe = false;
bool isUIActive = false;
volatile bool mqttConnected = false;
volatile bool snifferMode = false;
BLECharacteristic* pSnifferChar = NULL;

String relayCharUUIDs[NUM_RELAYS] = {
    "AAAA0001-0000-1000-8000-00805F9B0001",
    "AAAA0001-0000-1000-8000-00805F9B0002",
    "AAAA0001-0000-1000-8000-00805F9B0003",
    "AAAA0001-0000-1000-8000-00805F9B0004",
    "AAAA0001-0000-1000-8000-00805F9B0005",
    "AAAA0001-0000-1000-8000-00805F9B0006"
};
bool relayState[NUM_RELAYS] = {false};
BLECharacteristic* pRelayChars[NUM_RELAYS];

unsigned long sequenceTimer = 0;
int sequenceStep = 0;
unsigned long lastBlinkConfig = 0;
uint32_t lastColor = 0;

// ================================================================
//  HELPER SERIALE (Serial0 = debug USB)
// ================================================================
void safeSerialPrintf(const char *format, ...);
void safeSerialPrintln(const String& msg) { safeSerialPrintf("%s\n", msg.c_str()); }
void safeSerialPrintf(const char *format, ...) {
    if (xSemaphoreTake(serialMutex, (TickType_t)100) == pdTRUE) {
        va_list args;
        va_start(args, format);
        vprintf(format, args);
        va_end(args);
        xSemaphoreGive(serialMutex);
    }
}

// ================================================================
//  LED
// ================================================================
void setLedColor(uint8_t r, uint8_t g, uint8_t b, uint8_t brightness) {
    uint32_t c = pixels.Color((r*brightness)/255, (g*brightness)/255, (b*brightness)/255);
    if (c != lastColor) { pixels.setPixelColor(0, c); pixels.show(); lastColor = c; }
}

// ================================================================
//  BLE HELPERS
// ================================================================
void updateHubCharStatus(int index, String status) {
    if (index >= 0 && index < MAX_SENSORS && pHubChars[index] != NULL) {
        pHubChars[index]->setValue(status.c_str());
        pHubChars[index]->notify();
    }
}

String getNextSensorData(int index) {
    if (index < 0 || index >= MAX_SENSORS || !sensorDataReady[index]) return "";
    String data = "";
    if (xSemaphoreTake(dataMutex, (TickType_t)10) == pdTRUE) {
        data = sensorDataBuffer[index];
        sensorDataReady[index] = false;
        xSemaphoreGive(dataMutex);
    }
    return data;
}

// ================================================================
//  MODEM HELPERS — usati SOLO da mqttTask (Core 0)
// ================================================================

//Verifica la risposta ok, recuperando quelle mozzate
bool isOkResponse(const String& s) {
    if (s == "OK") return true;
    if (s.length() == 2 && s[1] == 'K') return true;
    if (s.length() == 2 && s[0] == 'O') return true;
    return false;
}

//Verifica la risposta SUBACK, recuperando quelle mozzate
bool isSubackResponse(const String& s) {
    if (s == "SUBACK") return true;
    if (s.length() == 6 && (s[1] == 'U' || s[2] == 'B' || s[3] == 'A')) return true;
    return false;
}

//Stampa in seriale USB ciò che si riceve dal modem
static void logModemLine(const String& line) {
    if (line.length() == 0) return;
    for (int i = 0; i < (int)line.length(); i++) {
        if ((uint8_t)line[i] >= 0x20 && (uint8_t)line[i] <= 0x7E) {
            safeSerialPrintf("[MODEM RX] '%s'\n", line.c_str());
            return;
        }
    }
}

static void flushSerial1() {
    while (Serial1.available()) Serial1.read();
}

bool waitModem(const String& expected, unsigned long timeout_ms) {
    unsigned long start = millis();
    String buf = "";
    while (millis() - start < timeout_ms) {
        while (Serial1.available()) {
            char c = Serial1.read();
            if (c == '\n') {
                buf.trim();
                logModemLine(buf);
                if (buf.indexOf(expected) >= 0) return true;
                if (expected == "OK" && isOkResponse(buf)) return true;
                buf = "";
            } else { buf += c; }
        }
        vTaskDelay(10 / portTICK_PERIOD_MS);
    }
    safeSerialPrintf("[MODEM] Timeout aspettando: '%s'\n", expected.c_str());
    return false;
}

bool waitModemAny(std::initializer_list<String> candidates, unsigned long timeout_ms) {
    unsigned long start = millis();
    String buf = "";
    while (millis() - start < timeout_ms) {
        while (Serial1.available()) {
            char c = Serial1.read();
            if (c == '\n') {
                buf.trim();
                logModemLine(buf);
                for (const String& cand : candidates) {
                    if (buf.indexOf(cand) >= 0)                    return true;
                    if (cand == "OK"     && isOkResponse(buf))     return true;
                    if (cand == "SUBACK" && isSubackResponse(buf)) return true;
                }
                buf = "";
            } else { buf += c; }
        }
        vTaskDelay(10 / portTICK_PERIOD_MS);
    }
    safeSerialPrintln("[MODEM] Timeout waitModemAny");
    return false;
}

// ================================================================
//  INIT MQTT
// ================================================================
void initMQTT() {
    mqttConnected = false;

    flushSerial1();

    Serial1.println("ATE0");
    vTaskDelay(300 / portTICK_PERIOD_MS);
    flushSerial1();
    Serial1.println("ATE0");
    vTaskDelay(300 / portTICK_PERIOD_MS);
    flushSerial1();

    safeSerialPrintln("[MQTT] Attendo registrazione rete...");

    unsigned long start = millis();
    bool registered = false;
    while (millis() - start < 30000) {
        Serial1.println("AT+CGREG?");
        String buf = "";
        unsigned long t = millis();
        while (millis() - t < 2000) {
            while (Serial1.available()) buf += (char)Serial1.read();
            vTaskDelay(10 / portTICK_PERIOD_MS);
        }
        if (buf.indexOf(",1") >= 0 || buf.indexOf(",5") >= 0) { registered = true; break; }
        safeSerialPrintln("[MQTT] Non ancora registrato, riprovo...");
        vTaskDelay(2000 / portTICK_PERIOD_MS);
    }
    if (!registered) { safeSerialPrintln("[MQTT] ERRORE: rete non disponibile!"); return; }

    safeSerialPrintln("[MQTT] Rete OK. Configuro MQTT...");

    Serial1.println("AT+MCONFIG=868488076771714,root,luat123456");
    if (!waitModem("OK", 5000)) return;

    Serial1.println("AT+MQTTMSGSET=0");
    if (!waitModem("OK", 3000)) return;

    Serial1.println("AT+MIPSTART=\"dms.gds.com\",\"1883\"");
    bool tcpAlready = false;
    {
        unsigned long t0 = millis();
        String buf = "";
        while (millis() - t0 < 15000) {
            while (Serial1.available()) {
                char c = Serial1.read();
                if (c == '\n') {
                    buf.trim();
                    logModemLine(buf);
                    if (buf.indexOf("CONNECT OK") >= 0)      { tcpAlready = false; goto tcp_ok; }
                    if (buf.indexOf("ALREADY CONNECT") >= 0) { tcpAlready = true;  goto tcp_ok; }
                    if (buf.indexOf("ERROR") >= 0)           { safeSerialPrintln("[MQTT] MIPSTART fallito"); return; }
                    buf = "";
                } else { buf += c; }
            }
            vTaskDelay(10 / portTICK_PERIOD_MS);
        }
        safeSerialPrintln("[MQTT] Timeout MIPSTART"); return;
    }
    tcp_ok:

    if (tcpAlready) {
        safeSerialPrintln("[MQTT] TCP gia' aperto, salto MCONNECT");
    } else {
        // keepalive 300s (5 minuti) per evitare disconnessioni frequenti
        Serial1.println("AT+MCONNECT=1," MQTT_KEEPALIVE_STR);
        if (!waitModem("CONNACK OK", 10000)) return;
    }

    Serial1.println("AT+MSUB=\"modem/invio\",0");
    waitModemAny({"SUBACK", "OK"}, 5000);

    vTaskDelay(300 / portTICK_PERIOD_MS);
    flushSerial1();

    mqttConnected = true;
    safeSerialPrintln("[MQTT] Connessione MQTT OK!");
}

// ================================================================
//  GESTIONE MESSAGGI IN ARRIVO DA MQTT
// ================================================================
void handleIncomingMQTT(const String& payload) {
    safeSerialPrintf("[CMD] ricevuto: '%s'\n", payload.c_str());

    if (payload.startsWith("relay:")) {
        int sep = payload.indexOf(':', 6);
        if (sep > 0) {
            int idx    = payload.substring(6, sep).toInt() - 1;
            String cmd = payload.substring(sep + 1);
            if (idx >= 0 && idx < NUM_RELAYS) {
                bool state = (cmd == "ON" || cmd == "1");
                relayState[idx] = state;
                digitalWrite(relayPins[idx], state ? HIGH : LOW);
                safeSerialPrintf("[RELAY] %d -> %s\n", idx+1, state?"ON":"OFF");

                // Notifica la web app via BLE
                if (pRelayChars[idx] != NULL) {
                    pRelayChars[idx]->setValue(state ? "1" : "0");
                    pRelayChars[idx]->notify();
                }
            }
        }
    }
}

// ================================================================
//  MOTORE DI PARSING DINAMICO
// ================================================================
String parseBeaconPayload(BLEAdvertisedDevice& dev, String rules) {
    String dataParts = "";
    int rssi = dev.getRSSI();

    if (rules.length() < 3) return "status=no_rule;rssi=" + String(rssi);

    int startIdx = 0;
    while (startIdx < rules.length()) {
        int endIdx = rules.indexOf(';', startIdx);
        if (endIdx == -1) endIdx = rules.length();
        
        String rule = rules.substring(startIdx, endIdx);
        startIdx = endIdx + 1;

        int p1 = rule.indexOf(',');
        int p2 = rule.indexOf(',', p1 + 1);
        int p3 = rule.indexOf(',', p2 + 1);
        int p4 = rule.indexOf(',', p3 + 1);
        int p5 = rule.indexOf(',', p4 + 1);

        if (p1 > 0 && p2 > 0 && p3 > 0 && p4 > 0 && p5 > 0) {
            String source   = rule.substring(0, p1);
            String targetId = rule.substring(p1 + 1, p2);
            int offset      = rule.substring(p2 + 1, p3).toInt();
            int len         = rule.substring(p3 + 1, p4).toInt();
            String opStr    = rule.substring(p4 + 1, p5); 
            String label    = rule.substring(p5 + 1);
            
            std::string payload;
            if (source == "MFR" && dev.haveManufacturerData()) {
                payload = dev.getManufacturerData();
            } else if (source == "SVC" && dev.haveServiceData()) {
                uint16_t uuid16 = (uint16_t) strtol(targetId.c_str(), NULL, 16);
                BLEUUID targetUUID(uuid16);
                int svcCount = dev.getServiceDataCount();
                for (int i = 0; i < svcCount; i++) {
                    if (dev.getServiceDataUUID(i).equals(targetUUID)) {
                        payload = dev.getServiceData(i);
                        break; 
                    }
                }
            }
            
            if (!payload.empty() && payload.size() >= (size_t)(offset + len)) {
                bool isBigEndian = false;
                bool isSigned = false;
                if (opStr.startsWith("B")) { isBigEndian = true; opStr.remove(0, 1); }
                if (opStr.startsWith("S")) { isSigned = true; opStr.remove(0, 1); }
                
                uint32_t rawVal = 0;
                for(int i=0; i<len; i++) {
                    uint8_t b = (uint8_t)payload[offset + i];
                    if (isBigEndian) rawVal = (rawVal << 8) | b;
                    else             rawVal = rawVal | (b << (i * 8));
                }
                
                float finalValue = 0;
                if (isSigned) {
                    int32_t signedVal = rawVal;
                    if (len == 1 && (rawVal & 0x80)) signedVal |= 0xFFFFFF00;
                    else if (len == 2 && (rawVal & 0x8000)) signedVal |= 0xFFFF0000;
                    else if (len == 3 && (rawVal & 0x800000)) signedVal |= 0xFF000000;
                    finalValue = (float)signedVal;
                } else {
                    finalValue = (float)rawVal;
                }
                
                if (opStr.startsWith("&")) {
                    finalValue = (long)finalValue & strtol(opStr.substring(1).c_str(), NULL, 10);
                } else if (opStr.startsWith(">>")) {
                    finalValue = (long)finalValue >> opStr.substring(2).toInt();
                } else if (opStr.startsWith("%")) {
                    int mod = opStr.substring(1).toInt();
                    if (mod != 0) finalValue = (long)finalValue % mod;
                } else {
                    float div = opStr.toFloat();
                    if (div != 0) finalValue = finalValue / div; 
                }
                
                // AGGIUNGE IL DATO ALLA LISTA (es: temp=25.50;)
                dataParts += label + "=" + String(finalValue, 2) + ";";
            }
        }
    }
    
    // PREPARAZIONE STRINGA FINALE
    if (dataParts.length() > 0) {
        String mac = String(dev.getAddress().toString().c_str());
        mac.toUpperCase();
        String devName = dev.haveName() ? String(dev.getName().c_str()) : "Sconosciuto";
        
        // Risultato per la Web App: mac=...;name=...;temp=...;rssi=...
        return "mac=" + mac + ";name=" + devName + ";" + dataParts + "rssi=" + String(rssi);
    } else {
        return "status=parse_error;rssi=" + String(rssi);
    }
}

// ================================================================
//  SNIFFER CALLBACK (Ponte radio verso la Web App)
// ================================================================
class SnifferCallbacks: public BLEAdvertisedDeviceCallbacks {
    void onResult(BLEAdvertisedDevice dev) {
        if (!snifferMode || pSnifferChar == nullptr) return;

        String mac = dev.getAddress().toString().c_str();
        mac.toUpperCase();
        int rssi = dev.getRSSI();
        String dataStr = "";

        // Cattura i Manufacturer Data (MFR)
        if (dev.haveManufacturerData()) {
            std::string md = dev.getManufacturerData();
            dataStr = "MFR=";
            for (size_t i = 0; i < md.size(); i++) {
                char hex[3]; sprintf(hex, "%02X", (uint8_t)md[i]); dataStr += hex;
            }
        } 
        // Oppure cattura i Service Data (SVC)
        else if (dev.haveServiceData()) {
                int count = dev.getServiceDataCount();
            if (count > 0) {
                BLEUUID uuid = dev.getServiceDataUUID(0);
                std::string sd = dev.getServiceData(0);
                // Estrae i 4 caratteri centrali dell'UUID (es. 2a6e)
                dataStr = "SVC=" + String(uuid.toString().c_str()).substring(4,8) + "=";
                for (size_t i = 0; i < sd.size(); i++) {
                    char hex[3]; sprintf(hex, "%02X", (uint8_t)sd[i]); dataStr += hex;
                }
            }
        }

        // Se ha trovato qualcosa, lo spara alla Web App
        if (dataStr != "") {
            String msg = mac + ";" + String(rssi) + ";" + dataStr;
            pSnifferChar->setValue(msg.c_str());
            pSnifferChar->notify();
        }
    }
};

SnifferCallbacks snifferCallbacksObj;

class SnifferControlCallback: public BLECharacteristicCallbacks {
    void onWrite(BLECharacteristic *pC) {
        String value = pC->getValue();
        if (value == "1") {
            snifferMode = true;
            safeSerialPrintln(">>> SNIFFER MODE ATTIVATA DALLA WEB APP <<<");
            BLEDevice::getScan()->setAdvertisedDeviceCallbacks(new SnifferCallbacks(), true);
        } else {
            snifferMode = false;
            safeSerialPrintln(">>> SNIFFER MODE DISATTIVATA <<<");
        }
    }
};


// ================================================================
//  CALLBACKS BLE SERVER
// ================================================================
class MyServerCallbacks: public BLEServerCallbacks {
    void onConnect(BLEServer* pServer)    { deviceConnectedToMe=true;  safeSerialPrintln(">>> CLIENT CONNESSO <<<"); }
    void onDisconnect(BLEServer* pServer) { deviceConnectedToMe=false; safeSerialPrintln(">>> CLIENT DISCONNESSO. Riavvio Adv... <<<"); pServer->getAdvertising()->start(); }
};

class ConfigCallbacks: public BLECharacteristicCallbacks {
    String keyName;
public:
    ConfigCallbacks(String key) { keyName=key; }
    void onWrite(BLECharacteristic *pC) {
        String value=pC->getValue();
        if (value.length()>0) {
            if (keyName.startsWith("mac_")) value.toLowerCase();
            safeSerialPrintf("!!! CONFIG: %s = %s !!!\n",keyName.c_str(),value.c_str());
            preferences.begin("ble-config",false); preferences.putString(keyName.c_str(),value); preferences.end();
            pixels.setPixelColor(0,pixels.Color(50,50,0)); pixels.show(); delay(50); lastColor=0;
        }
    }
};

class SensorCountCallback: public BLECharacteristicCallbacks {
    void onWrite(BLECharacteristic *pC) {
        String value=pC->getValue();
        if (value.length()>0) {
            int c=constrain(value.toInt(),1,MAX_SENSORS);
            preferences.begin("ble-config",false); preferences.putInt("active_count",c); preferences.end();
            safeSerialPrintf("!!! COUNT: %d -> REBOOT !!!\n",c);
            pixels.setPixelColor(0,pixels.Color(50,0,50)); pixels.show(); delay(1000); ESP.restart();
        }
    }
};

class HubNameCallback: public BLECharacteristicCallbacks {
    void onWrite(BLECharacteristic *pC) {
        String value=pC->getValue();
        if (value.length()>0) {
            hubName=value;
            preferences.begin("ble-config",false); preferences.putString("hub_name",hubName); preferences.end();
            safeSerialPrintf("!!! NAME: %s !!!\n",hubName.c_str());
            pixels.setPixelColor(0,pixels.Color(0,0,50)); pixels.show(); delay(200); lastColor=0;
        }
    }
};

class ResetDataCallback: public BLECharacteristicCallbacks {
    void onWrite(BLECharacteristic *pC) {
        if (pC->getValue()=="1") {
            preferences.begin("ble-config",false);
            for(int i=0;i<MAX_SENSORS;i++){
                preferences.remove(("s_uuid_"+String(i)).c_str());
                preferences.remove(("c_uuid_"+String(i)).c_str());
                preferences.remove(("mac_"+String(i)).c_str());
            }
            preferences.end();
            pC->setValue("0"); pC->notify();
            safeSerialPrintln(">>> FACTORY RESET OK.");
            pixels.setPixelColor(0,pixels.Color(0,50,50)); pixels.show(); delay(500); lastColor=0;
        }
    }
};

class RelayWriteCallback: public BLECharacteristicCallbacks {
    int relayIndex;
public:
    RelayWriteCallback(int index) { relayIndex=index; }
    void onWrite(BLECharacteristic *pC) {
        String value=pC->getValue();
        if (value.length()>0) {
            bool ns=(value=="1");
            relayState[relayIndex]=ns;
            digitalWrite(relayPins[relayIndex],ns?HIGH:LOW);
            safeSerialPrintf(">>> RELE %d [GPIO %d] -> %s\n",relayIndex+1,relayPins[relayIndex],ns?"ON":"OFF");
        }
    }
};

// ================================================================
//  SCANNER BEACON
// ================================================================
class MyAdvertisedDeviceCallbacks: public BLEAdvertisedDeviceCallbacks {
    void onResult(BLEAdvertisedDevice advertisedDevice) {
        String foundMac = advertisedDevice.getAddress().toString().c_str();
        foundMac.toLowerCase();
        
        for (int i = 0; i < activeSensors; i++) {
            if (targetMacAddresses[i].length() < 17) continue;
            if (foundMac != targetMacAddresses[i]) continue;
            
            // La funzione parseBeaconPayload genera già la stringa con MAC e NOME
            String dataStr = parseBeaconPayload(advertisedDevice, targetRules[i]);
            
            if (dataStr.length() > 0 && !dataStr.startsWith("status=")) {
                // 1. AGGIORNAMENTO STATI (Corretto: lastSeenTime invece di lastContact)
                connected[i] = true; 
                lastSeenTime[i] = millis(); 

                // 2. INVIO ALLA WEB APP (Bluetooth) - Stringa COMPLETA
                // Usiamo pHubChars[i] invece del segnaposto charX
                if (pHubChars[i] != nullptr) {
                    pHubChars[i]->setValue(dataStr.c_str());
                    pHubChars[i]->notify();
                }

                // 3. PULIZIA PER MQTT (Togliamo i primi due campi: mac=... e name=...)
                String cleanData = dataStr;
                int firstSemi = cleanData.indexOf(';'); // Fine del campo MAC
                if (firstSemi > 0) {
                    int secondSemi = cleanData.indexOf(';', firstSemi + 1); // Fine del campo NAME
                    if (secondSemi > 0) {
                        // Tagliamo la stringa per tenere solo i dati (es: temp=25;rssi=-70)
                        cleanData = cleanData.substring(secondSemi + 1);
                    }
                }

                // 4. SALVATAGGIO DATI PULITI (Per il task MQTT)
                if (xSemaphoreTake(dataMutex, (TickType_t)10) == pdTRUE) {
                    sensorDataBuffer[i] = cleanData;
                    sensorDataReady[i] = true;
                    xSemaphoreGive(dataMutex);
                }
                
                safeSerialPrintf("[BEACON] Slot %d | %s\n", i + 1, cleanData.c_str());
            }
            return;
        }
    }
};

// ================================================================
//  TASK BLE — Core 0
// ================================================================
void bleTask(void* parameter) {
    safeSerialPrintf("BLE Task su Core %d\n", xPortGetCoreID());
    for (;;) {
        if (currentMode == 1) {
            if (snifferMode && !BLEDevice::getScan()->isScanning()) {
                BLEDevice::getScan()->start(1, false); // Scansiona per 1 secondi
                BLEDevice::getScan()->clearResults();  // Svuota la RAM per evitare crash!
            }
            vTaskDelay(100 / portTICK_PERIOD_MS);
            continue;
        }
        if (!BLEDevice::getScan()->isScanning())
            BLEDevice::getScan()->start(5,false);
        unsigned long now=millis();
        for(int i=0;i<activeSensors;i++){
            if (targetMacAddresses[i].length()<17) continue;
            if (connected[i]&&lastSeenTime[i]>0&&(now-lastSeenTime[i])>SENSOR_TIMEOUT_MS) {
                connected[i]=false; updateHubCharStatus(i,"Disconnected");
                safeSerialPrintf("[TIMEOUT] Slot %d offline.\n",i+1);
            }
        }
        vTaskDelay(500/portTICK_PERIOD_MS);
    }
}

// ================================================================
//  TASK MQTT — Core 0
//
//  Architettura:
//  - Un unico listener legge Serial1 ogni ciclo (no race condition)
//  - Publish con delay 150ms tra uno e l'altro (no raffica al modem)
//  - Errore 767: riconnette solo MQTT (non reinit completo)
//  - Keepalive 300s: evita disconnessioni per inattività
// ================================================================
void mqttTask(void* parameter) {
    safeSerialPrintf("MQTT Task su Core %d\n", xPortGetCoreID());
    vTaskDelay(3000/portTICK_PERIOD_MS);

    static String rxBuf = "";
    static unsigned long rxLastChar = 0;

    for (;;) {
        // --- RICONNESSIONE COMPLETA ---
        if (currentMode != 1 && !mqttConnected) {
            rxBuf = "";
            initMQTT();
            if (!mqttConnected) vTaskDelay(5000/portTICK_PERIOD_MS);
            continue;
        }

        if (currentMode != 1 && mqttConnected) {

            // ── LISTENER: unico punto che legge Serial1 ──────────
            while (Serial1.available()) {
                char c = Serial1.read();
                rxLastChar = millis();
                if ((uint8_t)c < 0x20 && c != '\r' && c != '\n') continue;
                if (c == '\n') {
                    rxBuf.trim();
                    if (rxBuf.startsWith("+MSUB:")) {
                        // Messaggio MQTT in arrivo
                        safeSerialPrintf("[MQTT IN] %s\n", rxBuf.c_str());
                        int byteIdx = rxBuf.indexOf(" byte,");
                        if (byteIdx >= 0) {
                            String payload = rxBuf.substring(byteIdx + 6);
                            payload.trim();
                            safeSerialPrintf("[PAYLOAD] %s\n", payload.c_str());
                            handleIncomingMQTT(payload);
                        }
                    } else if (rxBuf.indexOf("CONNACK OK") >= 0) {
                        // Riconnessione MQTT riuscita (dopo errore 767)
                        safeSerialPrintln("[MQTT] Riconnesso MQTT OK");
                        // Rifai la subscribe dopo riconnessione
                        Serial1.println("AT+MSUB=\"modem/invio\",0");
                    } else if (rxBuf.indexOf("767") >= 0) {
                        // Sessione MQTT caduta, TCP ancora up → riconnetti solo MQTT
                        safeSerialPrintln("[MQTT] Sessione caduta (767), riconnetto MQTT...");
                        Serial1.println("AT+MCONNECT=1," MQTT_KEEPALIVE_STR);
                        // NON bloccare qui — il CONNACK OK arriva nel listener
                    } else if (rxBuf.indexOf("ERROR") >= 0) {
                        // Errore grave (non 767) → reinit completo
                        safeSerialPrintf("[MODEM] Errore grave: %s\n", rxBuf.c_str());
                        mqttConnected = false;
                    }
                    rxBuf = "";
                } else {
                    rxBuf += c;
                }
            }
            // Scarta righe incomplete rimaste ferme da >100ms
            if (rxBuf.length() > 0 && millis() - rxLastChar > 100) {
                rxBuf = "";
            }

            String rs;
            for (int i=0;i<NUM_RELAYS;i++){
                rs = rs + relayState[i];
            }
           
            //  PUBLISH: con delay 150ms tra un publish e l'altro ─
            for (int i = 0; i < activeSensors; i++) {
                String nd = getNextSensorData(i);
                if (nd == "") continue;
                String msg = String(hubName) + ":" + String(i+1) + ":" + nd + "|" + rs;
                String cmd = "AT+MPUB=\"modem/display\",0,0,\"" + msg + "\"";
                Serial1.println(cmd);
                safeSerialPrintln("TX MQTT: " + cmd);
                vTaskDelay(150 / portTICK_PERIOD_MS); 
            }
        }
        vTaskDelay(20/portTICK_PERIOD_MS);
    }
}

// ================================================================
//  SETUP
// ================================================================
void setup() {
    Serial.begin(115200);
    for(int i=0;i<NUM_RELAYS;i++){ pinMode(relayPins[i],OUTPUT); digitalWrite(relayPins[i],LOW); }

    Serial1.setRxBufferSize(512);
    Serial1.begin(EXT_SERIAL_BAUD, SERIAL_8N1, EXT_SERIAL_RX, EXT_SERIAL_TX);

    serialMutex = xSemaphoreCreateMutex();
    dataMutex   = xSemaphoreCreateMutex();
    delay(1000);
    pinMode(BUTTON_BOOT, INPUT_PULLUP);
    pixels.begin(); pixels.show();

    for(int i=0;i<MAX_SENSORS;i++){ connected[i]=false; pHubChars[i]=NULL; lastSeenTime[i]=0; }

    preferences.begin("ble-config",false);
    activeSensors=preferences.getInt("active_count",4);
    hubName=preferences.getString("hub_name","Hub-01");
    activeSensors=constrain(activeSensors,1,MAX_SENSORS);
    esp_reset_reason_t reason=esp_reset_reason();
    if (reason==ESP_RST_POWERON) { currentMode=0; preferences.putInt("mode",0); }
    else currentMode=preferences.getInt("mode",0);
    preferences.end();

    if (currentMode==1) {
        BLEDevice::init("ESP32-S3-Hub-Config");
        BLEServer *pServer=BLEDevice::createServer();
        pServer->setCallbacks(new MyServerCallbacks());
        BLEService *pService=pServer->createService(BLEUUID(CONFIG_SERVICE_UUID),64);
        auto mkChar=[&](const char* uuid,BLECharacteristicCallbacks* cb,String val){
            BLECharacteristic* c=pService->createCharacteristic(uuid,
                BLECharacteristic::PROPERTY_WRITE|BLECharacteristic::PROPERTY_READ);
            c->setCallbacks(cb); c->setValue(val.c_str()); return c;
        };
        mkChar(CONFIG_COUNT_UUID,new SensorCountCallback(),String(activeSensors));
        mkChar(CONFIG_NAME_UUID, new HubNameCallback(),hubName);
        BLECharacteristic *pResetChar=pService->createCharacteristic(CONFIG_RESET_UUID,
        BLECharacteristic::PROPERTY_WRITE|BLECharacteristic::PROPERTY_READ|BLECharacteristic::PROPERTY_NOTIFY);
        pResetChar->setCallbacks(new ResetDataCallback()); pResetChar->setValue("0");
        pSnifferChar = pService->createCharacteristic(CONFIG_SNIFFER_UUID, 
        BLECharacteristic::PROPERTY_WRITE | BLECharacteristic::PROPERTY_NOTIFY);
        pSnifferChar->setCallbacks(new SnifferControlCallback());
        pSnifferChar->addDescriptor(new BLE2902());
        pSnifferChar->setValue("0");
        preferences.begin("ble-config",true);
        for(int i=0;i<activeSensors;i++){
            char uM[40],uR[40]; // UUID per MAC e RULE
            const char* fmt=(i<9)?"00000000-0000-0000-0000-0000000000%d%d":"00000000-0000-0000-0000-000000000%d%d";
            sprintf(uM,fmt,i+1,2); // Es: termina con 12 per Slot 1 MAC
            sprintf(uR,fmt,i+1,3); // Es: termina con 13 per Slot 1 RULE
            
            // Crea Caratteristica per il MAC Address
            mkChar(uM, new ConfigCallbacks("mac_" + String(i)), preferences.getString(("mac_" + String(i)).c_str(),""));
            // Crea Caratteristica per la Regola di Parsing
            mkChar(uR, new ConfigCallbacks("rule_" + String(i)), preferences.getString(("rule_" + String(i)).c_str(),""));
        }
        preferences.end();
        pService->start();
        BLEDevice::getAdvertising()->addServiceUUID(CONFIG_SERVICE_UUID);
        BLEDevice::getAdvertising()->setScanResponse(true);
        BLEDevice::startAdvertising();
        safeSerialPrintln(">>> MODALITA' CONFIGURAZIONE ATTIVA <<<");

    } else {
        BLEDevice::init(hubName.c_str());
        BLEServer *pHubServer=BLEDevice::createServer();
        pHubServer->setCallbacks(new MyServerCallbacks());
        BLEService *pRelayService=pHubServer->createService(RELAY_SERVICE_UUID);
        for(int i=0;i<NUM_RELAYS;i++){
            pRelayChars[i]=pRelayService->createCharacteristic(relayCharUUIDs[i].c_str(),
                BLECharacteristic::PROPERTY_READ|BLECharacteristic::PROPERTY_WRITE|BLECharacteristic::PROPERTY_NOTIFY);
            pRelayChars[i]->setCallbacks(new RelayWriteCallback(i));
            pRelayChars[i]->setValue("0"); pRelayChars[i]->addDescriptor(new BLE2902());
            digitalWrite(relayPins[i],LOW);
        }
        pRelayService->start();
        BLEService *pHubService=pHubServer->createService(HUB_SERVICE_UUID);
        BLECharacteristic *pNameChar=pHubService->createCharacteristic(CONFIG_NAME_UUID,BLECharacteristic::PROPERTY_READ);
        pNameChar->setValue(hubName.c_str());
        for(int i=0;i<activeSensors;i++){
            char uuid[40];
            if(i<9) sprintf(uuid,"EEEE0000-0000-0000-0000-0000000000%d0",i+1);
            else     sprintf(uuid,"EEEE0000-0000-0000-0000-000000000%d0",i+1);
            pHubChars[i]=pHubService->createCharacteristic(uuid,
                BLECharacteristic::PROPERTY_READ|BLECharacteristic::PROPERTY_NOTIFY);
            pHubChars[i]->addDescriptor(new BLE2902());
        }
        pHubService->start();
        BLEAdvertising *pAdv=BLEDevice::getAdvertising();
        pAdv->addServiceUUID(HUB_SERVICE_UUID); pAdv->addServiceUUID(RELAY_SERVICE_UUID);
        pAdv->setScanResponse(true); pAdv->start();
        preferences.begin("ble-config",true);
        for(int i=0;i<activeSensors;i++){
            targetMacAddresses[i]=preferences.getString(("mac_"+String(i)).c_str(),"");
            targetMacAddresses[i].toLowerCase();
            targetRules[i]=preferences.getString(("rule_"+String(i)).c_str(),"");

            safeSerialPrintf("[SLOT %d] MAC: '%s'\n",i+1,targetMacAddresses[i].c_str());
            updateHubCharStatus(i,targetMacAddresses[i].length()>=17?"Disconnected":"Not Configured");
        }
        preferences.end();
        BLEScan* pBLEScan=BLEDevice::getScan();
        pBLEScan->setAdvertisedDeviceCallbacks(new MyAdvertisedDeviceCallbacks(),true);
        pBLEScan->setActiveScan(false); pBLEScan->setInterval(100); pBLEScan->setWindow(99);
        safeSerialPrintf(">>> HUB ATTIVO | %s | %d sensori <<<\n",hubName.c_str(),activeSensors);
    }

    xTaskCreatePinnedToCore(bleTask,  "BLE_Task",  8192, NULL, 1, &bleTaskHandle,  0);
    xTaskCreatePinnedToCore(mqttTask, "MQTT_Task", 4096, NULL, 1, &mqttTaskHandle, 0);
}

// ================================================================
//  PULSANTE
// ================================================================
void checkModeSwitch() {
    static unsigned long pressTime=0;
    static bool buttonHeld=false;
    if (digitalRead(BUTTON_BOOT)==LOW) {
        if (!buttonHeld) { pressTime=millis(); buttonHeld=true; isUIActive=true; }
        unsigned long d=millis()-pressTime;
        if (d>200&&d<3000) setLedColor(255,165,0,LED_BRIGHTNESS);
        else if (d>=3000) {
            setLedColor(255,255,255,255); delay(500);
            preferences.begin("ble-config",false);
            preferences.putInt("mode",(currentMode==0)?1:0);
            preferences.end(); ESP.restart();
        }
    } else {
        if (buttonHeld) { buttonHeld=false; isUIActive=false; lastColor=0; }
    }
}

// ================================================================
//  LOOP — Core 1 (solo LED)
// ================================================================
void loop() {
    checkModeSwitch();
    if (isUIActive) { delay(20); return; }

    if (currentMode == 1) {
        if (deviceConnectedToMe) setLedColor(0,0,255,LED_BRIGHTNESS);
        else {
            if (millis()-lastBlinkConfig>500) {
                lastBlinkConfig=millis();
                static bool blink=false; blink=!blink;
                setLedColor(0,0,255,blink?LED_BRIGHTNESS:0);
            }
        }
    } else {
        unsigned long now=millis();
        static bool isFlashing=false;
        unsigned long wt=(sequenceStep<activeSensors)
            ?(isFlashing?FLASH_DURATION:INTER_FLASH_GAP):END_CYCLE_PAUSE;
        if (now-sequenceTimer>wt) {
            sequenceTimer=now;
            if (sequenceStep<activeSensors) {
                if (!isFlashing) {
                    bool cfg=(targetMacAddresses[sequenceStep].length()>=17);
                    if (!cfg)                         setLedColor(125,125,125,20);
                    else if (connected[sequenceStep]) setLedColor(0,255,0,LED_BRIGHTNESS);
                    else                              setLedColor(255,0,0,LED_BRIGHTNESS);
                    isFlashing=true;
                } else { setLedColor(0,0,0,0); isFlashing=false; sequenceStep++; }
            } else { sequenceStep=0; isFlashing=false; }
        }
    }
    delay(10);
}
