
export AGT_URL=amqp://localhost:5672/routage
rm /tmp/date*

for i in {0..4}
do
date > /tmp/date$i
go run ./send user.agt.routage.key=clef1 user.agt.routage.from=from@ici user.agt.routage.file=/tmp/date$i
done
