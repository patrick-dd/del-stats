#!/bin/bash

END=2000
START=1

echo " +++ cleaning +++ "

for ((i=START;i<=END;i++)); do 
    # HOME TEAM
    if [ -f ../data/game_data/home_$i.json ]; then
        was_bad_download=$(cat ../data/game_data/home_$i.json | grep -c '"shots":\[\]')
        if (( $was_bad_download == 1 )); then
            echo "Deleting home_$i" 
            rm ../data/game_data/home_$i.json
        fi
    fi    
    # AWAY TEAM
    if [ -f ../data/game_data/guest_$i.json ]; then
        was_bad_download=$(cat ../data/game_data/guest_$i.json | grep -c '"shots":\[\]')
        if (( $was_bad_download ==1 )); then
            echo "Deleting guest_$i" 
            rm ../data/game_data/guest_$i.json
        fi
    fi
    # GAME HEADER 
    if [ -f ../data/game_data/game_$i.json ]; then
        was_bad_download=$(cat ../data/game_data/game_$i.json | grep -c '"actualTimeName":"vor dem Spiel"')
        if (( $was_bad_download ==1 )); then
            echo "Deleting game_$i"
            rm ../data/game_data/game_$i.json
        fi
    fi

    # ROSTER
    if [ -f ../data/game_data/roster_$i.json ]; then
        was_bad_download=$(cat ../data/game_data/roster_$i.json | grep -c '"home":\[\]')
        if (( $was_bad_download ==1 )); then
            echo "Deleting roster_$i"
            rm ../data/game_data/roster_$i.json
        fi
    fi

    # TOP SCORERS
    if [ -f ../data/game_data/top-scorers_$i.json ]; then
        was_bad_download=$(cat ../data/game_data/top-scorers_$i.json | grep -c '"shots":\[\]')
        if  (( $was_bad_download ==1 )); then
            echo "Delting top-scorers_$i"
            rm ../data/game_data/top-scorers_$i.json
        fi
    fi
    
    # EVENTS
    if [ -f ../data/game_data/events_$i.json ]; then
    was_bad_download=$(cat ../data/game_data/events_$i.json | grep -c '"1":\[\]')
        if  (( $was_bad_download ==1 )); then
            echo "Deleting events_$i"
            rm ../data/game_data/events_$i.json
        fi
    fi

done

echo " +++ downloading +++ "

for ((i=START;i<=END;i++)); do
    # HOME TEAM
    if [ ! -f ../data/game_data/home_$i.json ]; then
        is_200_ok=$(curl -s -I 'https://www.del.org/live-ticker/matches/'$i'/team-stats-home.json' | grep -c 'HTTP/1.1 200 OK')
        if (( $is_200_ok == 1 )); then
            sleep 0.1
            curl -s 'https://www.del.org/live-ticker/matches/'$i'/team-stats-home.json' -o "../data/game_data/home_$i.json"
            sleep 0.1
            echo "Downloaded (home) $i"
        else
            echo " >>> bad download (home) $i"
        fi
    fi

    # AWAY TEAM
    if [ ! -f ../data/game_data/guest_$i.json ]; then
        is_200_ok=$(curl -s -I 'https://www.del.org/live-ticker/matches/'$i'/team-stats-guest.json' | grep -c 'HTTP/1.1 200 OK')
        sleep 0.1
        if (( $is_200_ok == 1 )); then
            curl -s 'https://www.del.org/live-ticker/matches/'$i'/team-stats-guest.json' -o "../data/game_data/guest_$i.json"
            sleep 0.1
            echo "Downloaded (guest) $i"
        else
            echo " >>> bad download (guest) $i"
        fi

    fi
    
    # GAME HEADER 
    if [ ! -f ../data/game_data/game_$i.json ]; then
        is_200_ok=$(curl -s -I 'https://www.del.org/live-ticker/matches/'$i'/game-header.json' | grep -c 'HTTP/1.1 200 OK')
        sleep 0.1
        if (( $is_200_ok == 1 )); then
            curl -s 'https://www.del.org/live-ticker/matches/'$i'/game-header.json' -o "../data/game_data/game_$i.json"
            sleep 0.1
            echo "Downloaded (header) $i"
        else
            echo " >>> bad download (header) $i"
        fi
    fi

    # ROSTER 
    if [ ! -f ../data/game_data/roster_$i.json ]; then
        is_200_ok=$(curl -s -I 'https://www.del.org/live-ticker/matches/'$i'/roster.json' | grep -c 'HTTP/1.1 200 OK')
        sleep 0.1
        if (( $is_200_ok == 1 )); then
            curl -s 'https://www.del.org/live-ticker/matches/'$i'/roster.json' -o "../data/game_data/roster_$i.json"
            sleep 0.1
             echo "Downloaded (roster) $i"
        else
            echo " >>> bad download (roster) $i"
        fi
    fi

    # TOP SCORERS 
    if [ ! -f ../data/game_data/top-scorers_$i.json ]; then
        is_200_ok=$(curl -s -I 'https://www.del.org/live-ticker/matches/'$i'/top-scorers.json' | grep -c 'HTTP/1.1 200 OK')
        sleep 0.1
        if (( $is_200_ok == 1 )); then
            curl -s 'https://www.del.org/live-ticker/matches/'$i'/top-scorers.json' -o "../data/game_data/top-scorers_$i.json"
            sleep 0.1
            echo "Downloaded (top scorers) $i"
        else
            echo " >>> bad download (top scorers) $i"

        fi
    fi

    # PERIOD EVENTS
    if [ ! -f ../data/game_data/events_$i.json ]; then
        is_200_ok=$(curl -s -I 'https://www.del.org/live-ticker/matches/'$i'/period-events.json' | grep -c 'HTTP/1.1 200 OK')
        sleep .5
        if (( $is_200_ok == 1 )); then
            curl -s 'https://www.del.org/live-ticker/matches/'$i'/period-events.json' -o "../data/game_data/events_$i.json"
            sleep 0.1
            echo "Downloaded (events) $i"
        else
            echo " >>> bad download (events) $i"
       fi
    fi

done

# SHOT DATA

for ((i=START;i<END;i++)); do
    if [ -f ../data/shot_data/game_$i.json ]; then
        was_bad_download=$(cat ../data/shot_data/game_$i.json | grep -c '"shots":\[\]')
        if (($was_bad_download==1)); then
            echo "Deleting $i"
            rm ../data/shot_data/game_$i.json
        fi
    fi
done

for ((i=START;i<=END;i++)); do

    if [ ! -f ../data/shot_data/game_$i.json ]; then
        is_200_ok=$(curl -s -I 'https://www.del.org/live-ticker/visualization/shots/'$i'.json' | grep -c 'HTTP/1.1 200 OK') 
        if [ $is_200_ok == 1 ]; then
            sleep 0.1
            curl -s 'https://www.del.org/live-ticker/visualization/shots/'$i'.json' -o "../data/shot_data/game_$i.json" 
            echo "Downloaded: $i"
        else
            echo " >>> Not 200 response: $i"
        fi
    fi

done
