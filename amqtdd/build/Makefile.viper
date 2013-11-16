
all: amqtdd.viper amqtdd_mqtts.viper

amqtdd.viper: *.c *.h 
	arm-linux-gcc -Wall -s -Os *.c -o amqtdd.viper

amqtdd_mqtts.viper: *.c *.h
	arm-linux-gcc -DMQTTS -Wall -s -Os *.c -o amqtdd_mqtts.viper



