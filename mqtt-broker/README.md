#mqtt-broker

mqtt-broker is mqtt server
* support Qos0,QoS1


 
 
 # mqtt-event
 |type|mean|
 |----|----|
 |connect| mqtt client connect|
 |subscribe| mqtt client request subscribe|
 |unsubscribe|mqtt client request unsubscribe|
 
 
 
# todo
* mqtt-event-queue for sub/pub mqtt-broker event
* mqtt-broker save `subscribe tree` snapshot to file,and reload it when restart
  
