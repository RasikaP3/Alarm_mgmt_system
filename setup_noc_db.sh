#!/bin/bash

echo "Starting MongoDB NOC Database Setup..."

DB_NAME="noc_alarm_system"
EVENT_FILE="data/network_events.json"

mongosh <<EOF

use $DB_NAME

// -------------------------------
// DELETE OLD DATABASE
// -------------------------------

db.dropDatabase()
print("Previous database cleared")

// -------------------------------
// CREATE COLLECTIONS
// -------------------------------

db.createCollection("alarms")
db.createCollection("devices")
db.createCollection("topology")
db.createCollection("services")
db.createCollection("customers")
db.createCollection("raw_events")

// NEW COLLECTIONS
db.createCollection("engineers")
db.createCollection("shift_schedule")

print("Collections created")

// -------------------------------
// INSERT CMDB DEVICES
// -------------------------------

db.devices.insertMany([

{device_id:"core-1", hostname:"core-1.lab.local", tier:"core", type:"router", ip:"10.0.0.1", vendor:"Cisco", model:"ASR9000", location:"DC1"},

{device_id:"agg-1", hostname:"agg-1.lab.local", tier:"aggregation", type:"router", ip:"10.0.0.2", vendor:"Juniper", model:"MX480", location:"DC1"},
{device_id:"agg-2", hostname:"agg-2.lab.local", tier:"aggregation", type:"router", ip:"10.0.0.3", vendor:"Juniper", model:"MX480", location:"DC1"},

{device_id:"acc-1", hostname:"acc-1.lab.local", tier:"access", type:"switch", ip:"10.0.0.4", vendor:"Cisco", model:"Nexus9000", location:"DC1"},
{device_id:"acc-2", hostname:"acc-2.lab.local", tier:"access", type:"switch", ip:"10.0.0.5", vendor:"Cisco", model:"Nexus9000", location:"DC1"},
{device_id:"acc-3", hostname:"acc-3.lab.local", tier:"access", type:"switch", ip:"10.0.0.6", vendor:"Cisco", model:"Nexus9000", location:"DC1"},
{device_id:"acc-4", hostname:"acc-4.lab.local", tier:"access", type:"switch", ip:"10.0.0.7", vendor:"Cisco", model:"Nexus9000", location:"DC1"}

])

print("Devices inserted")

// -------------------------------
// INSERT TOPOLOGY
// -------------------------------

db.topology.insertMany([

{source:"core-1", destination:"agg-1"},
{source:"core-1", destination:"agg-2"},

{source:"agg-1", destination:"acc-1"},
{source:"agg-1", destination:"acc-2"},

{source:"agg-2", destination:"acc-3"},
{source:"agg-2", destination:"acc-4"}

])

print("Topology inserted")

// -------------------------------
// INSERT SERVICES
// -------------------------------

db.services.insertMany([

{
service_id:"SVC-1001",
service_name:"Internet Service",
service_type:"Broadband",
devices:["core-1","agg-1","acc-1"]
},

{
service_id:"SVC-1002",
service_name:"Cloud VPN",
service_type:"VPN",
devices:["core-1","agg-2","acc-3"]
},

{
service_id:"SVC-1003",
service_name:"Enterprise Network",
service_type:"Private Network",
devices:["core-1","agg-1","acc-2"]
}

])

print("Services inserted")

// -------------------------------
// INSERT CUSTOMERS
// -------------------------------

db.customers.insertMany([

{
customer_id:"CUST-001",
customer_name:"ABC Telecom",
service_id:"SVC-1001",
location:"New York"
},

{
customer_id:"CUST-002",
customer_name:"XYZ Enterprises",
service_id:"SVC-1002",
location:"San Francisco"
},

{
customer_id:"CUST-003",
customer_name:"Cloud Corp",
service_id:"SVC-1003",
location:"London"
}

])

print("Customers inserted")

// -------------------------------
// INSERT ENGINEERS
// -------------------------------

db.engineers.insertMany([

{
name:"Priya",
location:"Pune",
skills:["Switch","LAN"],
mobile:"9876543210",
email:"priya@company.com",
workload:2,
clock_in:true
},

{
name:"Amit",
location:"Mumbai",
skills:["Router","VPN"],
mobile:"9876543211",
email:"amit@company.com",
workload:1,
clock_in:true
},

{
name:"Rahul",
location:"Pune",
skills:["Firewall","Security"],
mobile:"9876543212",
email:"rahul@company.com",
workload:3,
clock_in:true
}

])

print("Engineers inserted")

// -------------------------------
// INSERT SHIFT SCHEDULE
// -------------------------------

db.shift_schedule.insertMany([

{engineer:"Amit", shift:"Morning"},
{engineer:"Priya", shift:"Morning"},
{engineer:"Rahul", shift:"Night"}

])

print("Shift schedule inserted")

// -------------------------------
// CREATE INDEXES
// -------------------------------

db.devices.createIndex({device_id:1})
db.devices.createIndex({ip:1})

db.topology.createIndex({source:1})
db.topology.createIndex({destination:1})

db.services.createIndex({service_id:1})
db.services.createIndex({devices:1})

db.customers.createIndex({customer_id:1})
db.customers.createIndex({service_id:1})

db.raw_events.createIndex({timestamp:-1})
db.raw_events.createIndex({device_name:1})

db.engineers.createIndex({name:1})
db.shift_schedule.createIndex({engineer:1})

print("Indexes created")

EOF

echo "Importing generated events..."

mongoimport \
--db noc_alarm_system \
--collection raw_events \
--file $EVENT_FILE \
--jsonArray

echo "Data Import Completed"

mongosh <<EOF

use noc_alarm_system

print("Devices Count:")
db.devices.countDocuments()

print("Engineers Count:")
db.engineers.countDocuments()

print("Shift Schedule Count:")
db.shift_schedule.countDocuments()

print("Topology Count:")
db.topology.countDocuments()

print("Services Count:")
db.services.countDocuments()

print("Customers Count:")
db.customers.countDocuments()

print("Raw Events Count:")
db.raw_events.countDocuments()

print("Sample Event:")
db.raw_events.findOne()

EOF

echo "NOC Database Setup Completed"