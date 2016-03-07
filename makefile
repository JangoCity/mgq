all:compile

compile:
	go build mgq.go && mv mgq bin/;

clean:
	rm bin/mgq;
