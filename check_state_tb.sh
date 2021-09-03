#!/bin/bash
ret=$(ps aux | grep [/]bin/tiebreakers_switch.py | wc -l)
if [ $ret -eq 0 ]
then 
  echo "Rerunning tiebreakers_switch"
  sleep 1
  python3 /bin/tiebreakers_switch.py &>/dev/null & disown
  exit 1
else 
  echo "tiebreakers_switch already running!"
  exit 1
fi;
