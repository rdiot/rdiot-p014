/* ARTIK MQTT+Raspberry Pi Apache Kafka Cluster Bridge [P014] : http://rdiot.tistory.com/336 [RDIoT Demo] */

#include <Wire.h> 
#include <LiquidCrystal_I2C.h>
#include <DHT.h>
#include <SPI.h>
#include <Ethernet.h>
#include <PubSubClient.h>

#define DHTPIN 2
#define DHTTYPE DHT22 
unsigned long time;
char* msg;

int pinA = 4; // A
int pinB = 3; // B

LiquidCrystal_I2C lcd(0x27,16,2);
DHT dht(DHTPIN, DHTTYPE);

char message_buffer[100];
int cdsPin = A0; // cds

byte MAC_ADDRESS[] = {  0xFE, 0xED, 0xDE, 0xAD, 0xBE, 0xEF };
// IP address of MQTT server
byte MQTT_SERVER[] = { 192, 168, 0, 106 };

EthernetClient ethClient;
PubSubClient client(MQTT_SERVER, 1883, callback, ethClient);

void setup()
{
  lcd.init(); // initialize the lcd 
  lcd.backlight();
  lcd.print("start LCD1602");  

  pinMode(pinA, INPUT);
  pinMode(pinB, INPUT);  

  delay(1000);
  lcd.clear();
  
  if(Ethernet.begin(MAC_ADDRESS) == 0)
  {
    Serial.println("Failed to configure Ethernet using DHCP");
    return;
  }
        
}

void loop()
{
  if (!client.connected())
  {
    //client.connect("clientID", "mqtt_username", "mqtt_password");
    client.connect("sfo-arduino");
    client.publish("sfo/arduino/alive", "I'm alive!");
  }
    
  int cds = analogRead(cdsPin);
 
  int valueA = digitalRead(pinA);
  int valueB = digitalRead(pinB);
  int airGrade = -1;

  if(valueA == LOW && valueB == LOW)
  {
    airGrade = 0;
  }
  else if(valueA == LOW && valueB == HIGH)
  {
    airGrade = 1;
  }
  else if(valueA == HIGH && valueB == LOW)
  {
    airGrade = 2;   
  }
  else if(valueA == HIGH && valueB == HIGH)
  {
    airGrade = 3;
  }

  lcd.setCursor(0,0);
  lcd.print("Cds=" + (String)cds + " AirL="+airGrade+" ");

   switch(dht.read())
  {
    case DHT_OK:
      lcd.setCursor(0,1);
      lcd.print("T" + (String)dht.temperature + "'C "+"H" + (String)dht.humidity + "%" );
      break;
    case DHT_ERR_CHECK:
      lcd.setCursor(0,3);
      lcd.print("DHT CHECK ERROR  ");
      break;
    case DHT_ERR_TIMEOUT:
      lcd.setCursor(0,3);
      lcd.print("DHT TIMEOUT ERROR");
      break;
    default:
      lcd.setCursor(0,3);
      lcd.print("UNKNOWN ERROR    ");
      break;
    } 

  msg = dtostrf(dht.temperature, 5, 2, message_buffer); //온도
  client.publish("temperature", msg);
  
  msg = dtostrf(dht.humidity, 5, 2, message_buffer); //습도
  client.publish("humidity", msg);

  msg = dtostrf(cds, 3, 0, message_buffer); //조도
  client.publish("cds", msg); 

  msg = dtostrf(airGrade, 1, 0, message_buffer); //공기질수준
  client.publish("airGrade", msg); 
    
  // MQTT client loop processing
  client.loop();

  delay(1000); 
     
}

// Handles messages arrived on subscribed topic(s)
void callback(char* topic, byte* payload, unsigned int length) {
  
}
