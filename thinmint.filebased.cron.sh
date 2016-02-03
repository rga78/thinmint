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
# Make a backup of the data
#
d=`date '+%m%d%y'`
cp -R data data.$d

#
# Update accounts
#
echo ./mintclient.py --action getMintAccounts --mintuser xxx --mintpass xxx --outputfile=data/accounts.mint.json
./mintclient.py --action getMintAccounts --mintuser $mintuser --mintpass $mintpass --outputfile=data/accounts.mint.json

#
# Get new trans
#
echo ./mintclient.py --action getMintTransactions --mintuser xxx --mintpass xxx --outputfile=data/trans.mint.json
./mintclient.py --action getMintTransactions --mintuser $mintuser --mintpass $mintpass --outputfile=data/trans.mint.json

#
# Merge trans
#
echo ./mintclient.py --action mergeTransactions --mintfile=data/trans.mint.json --inputfile=data/trans.thinmint.json --outputfile=data/trans.thinmint.json
./mintclient.py --action mergeTransactions --mintfile=data/trans.mint.json --inputfile=data/trans.thinmint.json --outputfile=data/trans.thinmint.json

#
# Compose email with status update, new trans in need of ACK'ing
#
echo ./mintclient.py --action composeEmailSummary --transfile=data/trans.thinmint.json --accountsfile=data/accounts.mint.json --outputfile=data/email.txt
./mintclient.py --action composeEmailSummary --transfile=data/trans.thinmint.json --accountsfile=data/accounts.mint.json --outputfile=data/email.txt

# 
# Send email
#
echo ./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to robertgalderman@gmail.com --gmailuser . --gmailpassword .
./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to 'robertgalderman@gmail.com' --gmailuser "$gmailuser" --gmailpassword "$gmailpass"

echo ./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to ilana.bram@gmail.com --gmailuser . --gmailpassword .
./mintclient.py --action sendEmailSummary --inputfile=data/email.txt --to 'ilana.bram@gmail.com' --gmailuser "$gmailuser" --gmailpassword "$gmailpass"




