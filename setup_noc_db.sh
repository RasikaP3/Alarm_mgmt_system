#!/bin/bash
echo "Starting MongoDB NOC Database Setup..."

DB_NAME="noc_alarm_system"
EVENT_FILE="data/network_events.json"

mongosh <<EOF
use $DB_NAME
db.dropDatabase()
print("Database cleared")

db.createCollection("alarms")
db.createCollection("devices")
db.createCollection("topology")
db.createCollection("services")
db.createCollection("customers")
db.createCollection("engineers")
db.createCollection("shift_schedule")
print("Collections created")

# Insert devices
db.devices.insertMany([
  {device_id:"core-1", hostname:"core-1.lab.local", device_role:"core", type:"router", ip:"10.0.0.1", vendor:"Cisco", model:"ASR9000", location:"DC1"},
  {device_id:"agg-1", hostname:"agg-1.lab.local", device_role:"aggregation", type:"router", ip:"10.0.0.2", vendor:"Juniper", model:"MX480", location:"DC1"},
  {device_id:"agg-2", hostname:"agg-2.lab.local", device_role:"aggregation", type:"router", ip:"10.0.0.3", vendor:"Juniper", model:"MX480", location:"DC1"},
  {device_id:"acc-1", hostname:"acc-1.lab.local", device_role:"access", type:"switch", ip:"10.0.0.4", vendor:"Cisco", model:"Nexus9000", location:"DC1"},
  {device_id:"acc-2", hostname:"acc-2.lab.local", device_role:"access", type:"switch", ip:"10.0.0.5", vendor:"Cisco", model:"Nexus9000", location:"DC1"},
  {device_id:"acc-3", hostname:"acc-3.lab.local", device_role:"access", type:"switch", ip:"10.0.0.6", vendor:"Cisco", model:"Nexus9000", location:"DC1"},
  {device_id:"acc-4", hostname:"acc-4.lab.local", device_role:"access", type:"switch", ip:"10.0.0.7", vendor:"Cisco", model:"Nexus9000", location:"DC1"}
])
print("Devices inserted")

# Insert topology, services, customers, engineers, shifts (same as your previous script)...
# -- truncated for brevity, use your previous insertion code --

# Indexes
# db.raw_events.createIndex({createdAt:-1})
# db.raw_events.createIndex({device_name:1})
db.alarms.createIndex({alarmId:1})
print("Indexes created")
EOF

# Import generated events (JSON array)
# mongoimport --db $DB_NAME --collection raw_events --file $EVENT_FILE --jsonArray
# echo "Raw events imported"

# Quick validation
mongosh $DB_NAME --eval 'print("Raw Events Count:", db.raw_events.countDocuments()); db.raw_events.findOne();'
echo "NOC Database Setup Completed"