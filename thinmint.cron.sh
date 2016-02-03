#!/bin/sh
#
#


#
# usage
#
usage() {
    bname=`basename $0`
    cat <<HERE 1>&2
Usage: $bname 

Downloads new trans from mint.com.
Merges with existing thinmint trans.
Compose and send email.
HERE
    if [ "$1" ] ; then echo " !! ERROR: $1 " 1>&2; fi
    exit 1
}


if [ "$1" = "-h" ]; then
    usage
fi

source ./.thinmint.env

if [ -z "$mintuser" ]; then
    usage "\$mintuser is not defined"
elif [ -z "$mintpass" ]; then
    usage "\$mintpass is not defined"
elif [ -z "$gmailuser" ]; then
    usage "\$gmailuser is not defined"
elif [ -z "$gmailpass" ]; then
    usage "\$gmailpass is not defined"
fi


#
# Get mint data
#
echo ./mintclient.py --action importMintDataToMongo --mintuser xxx --mintpas xxx
./mintclient.py --action importMintDataToMongo --mintuser "$mintuser" --mintpas "$mintpass"

#
# Compose email with status update, new trans in need of ACK'ing
#
echo ./mintclient.py --action composeEmailSummary --outputfile=data/email.txt
./mintclient.py --action composeEmailSummary --outputfile=data/email.txt

# 
# Send email
#
# echo ./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to robertgalderman@gmail.com --gmailuser xxx --gmailpass xxx
# ./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to 'robertgalderman@gmail.com' --gmailuser "$gmailuser" --gmailpass "$gmailpass"
# 
# echo ./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to ilana.bram@gmail.com --gmailuser xxx --gmailpass xxx 
# ./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to 'ilana.bram@gmail.com' --gmailuser "$gmailuser" --gmailpass "$gmailpass"




