# Omada_45_Omadikh_Ergasia
Εφαρμογή Συστήματος Μεταφοράς Ηλεκτρικής Ενέργειας 
--------------------------------------------------
Το project αποτελείται από 1 .py file,11 .csv files και τη βάση που υλοποιήθηκε 
με τη βοήθεια amazon web services. Το αρχείο .py δημιουργει και 
διαχειριζεται τη βαση και τα 11 csv files περιεχουν τα δεδομενα που 
συλλεξαμε. 

Η δημιουργια της βασης και το συμπληρωμα της απο δεδομενα εχει γινει
απο εμας παρολα αυτα σας τα παραθετουμε.
1. Ανοιγουμε το λογαριασμο aws μεσω του λινκ https://signin.aws.amazon.com/signin?redirect_uri=https%3A%2F%2Fconsole.aws.amazon.com%2Fconsole%2Fhome%3Fnc2%3Dh_ct%26src%3Dheader-signin%26state%3DhashArgs%2523%26isauthcode%3Dtrue&client_id=arn%3Aaws%3Aiam%3A%3A015428540659%3Auser%2Fhomepage&forceMobileApp=0&code_challenge=9tF2u20_RDX6M996qKgZtIsTkMbBEnaqoc9MO-Aoh5c&code_challenge_method=SHA-256
και συνδεομαστε στο IAM user με τα στοιχεια
IAM user name: Jimvlaxodimitropoulos
pass: 2105028552jimis!

2. Επειτα συνδεομαστε στο MySQL Workbench, δημιουργουμε συνδεση με 
τα στοιχεια

hostname: uni-project.cp2ldanyjuww.us-east-2.rds.amazonaws.com
port:3306
Username:admin

3. Ανοιγουμε ενα terminal και τρεχουμε την εντολη -pip install mysql-connector-python

4. Τελος ανοιγουμε και τρεχουμε το αρχειο .py και ακολουθουμε
τις οδηγιες του τερματικου
