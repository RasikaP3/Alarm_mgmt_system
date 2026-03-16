from insert_alarm import insert_alarm

ip = "10.59.102.113"
source = "syslog"
message = "Interface GigabitEthernet0/1 down"

insert_alarm(ip, source, message)