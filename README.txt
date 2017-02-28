Fabien Bessez
Assign4 : Consensus and Election

Instructions:
	Make sure to start a viewleader with 'python viewleader.py'. 
		There is the option to enter in 'python viewleader.py --viewleader REPLICAS_GO_HERE'
		where REPLICAS_GO_HERE is a comma delimited string without spaces. for example:
			localhost:39000,localhost:39001,localhost:39002 and so on
	Then perform whichever client or server RPCs that you would like.
	The replicas will adjust their logs accordingly. However, if you kill enough of the 
		replicas that a quorom will not be met by the remaining 'alive' replicas, then none 
		of the commands will work until you reboot the downed replicas. Note: Some Starter Code provided.