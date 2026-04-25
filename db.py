import sqlite3
import csv
import os
import logging

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "health.db")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = get_connection()
    cursor = conn.cursor()

    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS facilities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state TEXT,
            district TEXT,
            facility_name TEXT,
            facility_type TEXT,
            pincode TEXT,
            address TEXT,
            phone TEXT,
            beds INTEGER,
            emergency_available TEXT,
            latitude REAL,
            longitude REAL
        );

        CREATE TABLE IF NOT EXISTS symptoms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            disease TEXT,
            symptoms TEXT,
            first_aid_advice TEXT,
            recommended_facility TEXT,
            urgency_timeframe TEXT,
            needs_ambulance TEXT,
            severity TEXT
        );

        CREATE TABLE IF NOT EXISTS schemes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scheme_name TEXT,
            eligibility TEXT,
            benefit TEXT,
            where_available TEXT,
            how_to_apply TEXT,
            website TEXT
        );

        CREATE TABLE IF NOT EXISTS emergency_contacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state_ut TEXT,
            service TEXT,
            number TEXT,
            description TEXT
        );
    """)

    conn.commit()
    conn.close()
    logger.info("Database schema initialized.")
    _seed_data()


def _seed_data():
    conn = get_connection()
    cursor = conn.cursor()

    # Only seed if tables are empty
    if cursor.execute("SELECT COUNT(*) FROM facilities").fetchone()[0] > 0:
        conn.close()
        return

    facilities = [
        ("Uttar Pradesh","Agra","PHC Fatehpur Sikri","PHC","282001","Fatehpur Sikri Road, Agra","05612-230011",6,"No",27.0945,77.6602),
        ("Uttar Pradesh","Agra","CHC Kiraoli","CHC","282002","Kiraoli Block, Agra","05612-231456",30,"Yes",27.1500,77.8900),
        ("Uttar Pradesh","Agra","District Hospital Agra","District Hospital","282001","MG Road, Agra","0562-2260011",200,"Yes",27.1767,78.0081),
        ("Uttar Pradesh","Lucknow","PHC Malihabad","PHC","226101","Malihabad Block, Lucknow","0522-2840011",6,"No",26.9200,80.7100),
        ("Uttar Pradesh","Lucknow","CHC Bakshi Ka Talab","CHC","226201","BKT Block, Lucknow","0522-2841200",30,"Yes",26.9800,80.9500),
        ("Uttar Pradesh","Varanasi","PHC Chiraigaon","PHC","221101","Chiraigaon Block, Varanasi","0542-2500011",6,"No",25.3500,82.9700),
        ("Uttar Pradesh","Varanasi","CHC Cholapur","CHC","221105","Cholapur Block, Varanasi","0542-2501200",30,"Yes",25.2800,83.0400),
        ("Uttar Pradesh","Varanasi","District Hospital BHU","District Hospital","221005","Lanka, Varanasi","0542-2307111",500,"Yes",25.2677,82.9913),
        ("Bihar","Patna","PHC Phulwarisharif","PHC","801505","Phulwari Block, Patna","0612-2274011",6,"No",25.5700,85.1000),
        ("Bihar","Patna","CHC Danapur","CHC","801503","Danapur, Patna","0612-2274500",30,"Yes",25.6150,85.0400),
        ("Bihar","Patna","PMCH Patna","District Hospital","800004","Ashok Rajpath, Patna","0612-2300011",1500,"Yes",25.6093,85.1376),
        ("Bihar","Gaya","PHC Bodh Gaya","PHC","824234","Bodh Gaya Block, Gaya","0631-2200011",6,"No",24.6960,84.9910),
        ("Bihar","Gaya","CHC Sherghati","CHC","824201","Sherghati Block, Gaya","0631-2201456",30,"Yes",24.5600,84.8100),
        ("Bihar","Muzaffarpur","PHC Kanti","PHC","843108","Kanti Block, Muzaffarpur","0621-2240011",6,"No",26.1400,85.2700),
        ("Bihar","Muzaffarpur","District Hospital Muzaffarpur","District Hospital","842001","Hospital Road, Muzaffarpur","0621-2240511",300,"Yes",26.1209,85.3647),
        ("Rajasthan","Jaipur","PHC Chomu","PHC","303702","Chomu Block, Jaipur","01423-220011",6,"No",27.1600,75.7200),
        ("Rajasthan","Jaipur","CHC Shahpura","CHC","303103","Shahpura Block, Jaipur","01429-220100",30,"Yes",27.3900,75.9600),
        ("Rajasthan","Jaipur","SMS Hospital Jaipur","District Hospital","302004","JLN Marg, Jaipur","0141-2518888",2500,"Yes",26.9124,75.7873),
        ("Rajasthan","Jodhpur","PHC Balesar","PHC","342312","Balesar Block, Jodhpur","02925-250011",6,"No",27.0200,72.9800),
        ("Rajasthan","Jodhpur","CHC Osian","CHC","342303","Osian Block, Jodhpur","02922-256100",30,"Yes",26.7300,72.9100),
        ("Rajasthan","Udaipur","PHC Mavli","PHC","313203","Mavli Block, Udaipur","02955-230011",6,"No",24.7800,73.8800),
        ("Rajasthan","Udaipur","District Hospital Udaipur","District Hospital","313001","Hospital Road, Udaipur","0294-2428191",700,"Yes",24.5854,73.7125),
        ("Madhya Pradesh","Bhopal","PHC Berasia","PHC","462038","Berasia Block, Bhopal","0755-2710011",6,"No",23.6300,77.4300),
        ("Madhya Pradesh","Bhopal","CHC Huzur","CHC","462026","Huzur Block, Bhopal","0755-2711200",30,"Yes",23.1800,77.4600),
        ("Madhya Pradesh","Bhopal","Hamidia Hospital Bhopal","District Hospital","462001","Royal Market, Bhopal","0755-2540222",1200,"Yes",23.2599,77.4126),
        ("Madhya Pradesh","Indore","PHC Depalpur","PHC","453551","Depalpur Block, Indore","07322-250011",6,"No",22.8500,75.5400),
        ("Madhya Pradesh","Indore","CHC Sanwer","CHC","453551","Sanwer Block, Indore","07322-251200",30,"Yes",22.9700,75.8300),
        ("Maharashtra","Pune","PHC Mulshi","PHC","412108","Mulshi Block, Pune","020-23390011",6,"No",18.5200,73.5000),
        ("Maharashtra","Pune","CHC Maval","CHC","410506","Maval Block, Pune","02114-250100",30,"Yes",18.7600,73.6700),
        ("Maharashtra","Pune","Sassoon Hospital Pune","District Hospital","411001","Sassoon Road, Pune","020-26128000",1200,"Yes",18.5236,73.8674),
        ("Maharashtra","Nashik","PHC Igatpuri","PHC","422403","Igatpuri Block, Nashik","02553-244011",6,"No",19.6900,73.5600),
        ("Maharashtra","Nashik","District Hospital Nashik","District Hospital","422001","Pathardi Road, Nashik","0253-2576000",600,"Yes",20.0059,73.7897),
        ("Odisha","Bhubaneswar","PHC Jatni","PHC","752050","Jatni Block, Khordha","0674-2492011",6,"No",20.1700,85.7000),
        ("Odisha","Bhubaneswar","CHC Balianta","CHC","752101","Balianta Block, Khordha","0674-2493200",30,"Yes",20.2800,85.7400),
        ("Odisha","Bhubaneswar","SCB Medical Cuttack","District Hospital","753007","Manglabag, Cuttack","0671-2411780",1500,"Yes",20.4625,85.8830),
        ("West Bengal","Kolkata","PHC Baruipur","PHC","743302","Baruipur Block, South 24 PGS","033-24380011",6,"No",22.3600,88.4300),
        ("West Bengal","Kolkata","CHC Bishnupur","CHC","722122","Bishnupur Block, Bankura","03244-255100",30,"Yes",23.0800,87.3200),
        ("West Bengal","Kolkata","NRS Medical College Kolkata","District Hospital","700014","AJC Bose Road, Kolkata","033-22443100",1300,"Yes",22.5726,88.3639),
        ("Tamil Nadu","Chennai","PHC Sholinganallur","PHC","600119","Sholinganallur, Chennai","044-24501100",6,"No",12.9010,80.2279),
        ("Tamil Nadu","Chennai","CHC Tambaram","CHC","600045","Tambaram, Chennai","044-22261200",50,"Yes",12.9249,80.1000),
        ("Tamil Nadu","Chennai","Rajiv Gandhi Govt Hospital","District Hospital","600003","Park Town, Chennai","044-25305000",2600,"Yes",13.0827,80.2707),
        ("Tamil Nadu","Madurai","PHC Melur","PHC","625106","Melur Block, Madurai","04543-252011",6,"No",10.0400,78.3400),
        ("Tamil Nadu","Madurai","District Hospital Madurai","District Hospital","625020","EVR Road, Madurai","0452-2532010",600,"Yes",9.9252,78.1198),
        ("Karnataka","Bengaluru","PHC Anekal","PHC","562106","Anekal Block, Bengaluru","080-27842011",6,"No",12.7100,77.6900),
        ("Karnataka","Bengaluru","CHC Attibele","CHC","562107","Attibele Block, Bengaluru","080-27843200",30,"Yes",12.7800,77.7700),
        ("Karnataka","Bengaluru","Victoria Hospital Bengaluru","District Hospital","560002","Kalasipalya, Bengaluru","080-26703600",1600,"Yes",12.9716,77.5946),
        ("Karnataka","Mysuru","PHC Nanjangud","PHC","571301","Nanjangud Block, Mysuru","08221-226011",6,"No",12.1200,76.6800),
        ("Karnataka","Mysuru","District Hospital Mysuru","District Hospital","570001","Irwin Road, Mysuru","0821-2523900",700,"Yes",12.2958,76.6394),
        ("Gujarat","Ahmedabad","PHC Dholka","PHC","387810","Dholka Block, Ahmedabad","02714-220011",6,"No",22.4400,72.4700),
        ("Gujarat","Ahmedabad","CHC Daskroi","CHC","382430","Daskroi Block, Ahmedabad","079-22950100",30,"Yes",22.9700,72.6500),
        ("Gujarat","Ahmedabad","Civil Hospital Ahmedabad","District Hospital","380016","Asarwa, Ahmedabad","079-22681000",1800,"Yes",23.0600,72.5900),
    ]

    cursor.executemany("""
        INSERT INTO facilities (state,district,facility_name,facility_type,pincode,address,phone,beds,emergency_available,latitude,longitude)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, facilities)

    symptoms = [
        ("Fever","High body temperature above 38°C","Drink fluids, take paracetamol, rest. Seek care if > 3 days.","PHC","1-3 days","No","High"),
        ("Malaria","Fever with chills and sweating cycles","Take anti-malarial as prescribed. Must visit PHC for blood test.","PHC","Same day","Yes","High"),
        ("Dengue","High fever, severe headache, rash, joint pain","No aspirin/ibuprofen. Drink fluids, monitor platelet count at PHC.","PHC","Same day","Yes","High"),
        ("Tuberculosis","Persistent cough > 2 weeks, blood in sputum, weight loss","Visit PHC for sputum test. Free DOTS treatment available.","PHC/CHC","Same day","Yes","High"),
        ("Diarrhoea","Loose watery stools, stomach cramps","ORS solution, continue feeding, visit PHC if not improving in 2 days.","PHC","1-2 days","No","Medium"),
        ("Cholera","Severe watery diarrhoea, vomiting, dehydration","Emergency ORS, immediate hospital visit required.","District Hospital","Immediate","Yes","Critical"),
        ("Typhoid","Continuous high fever, abdominal pain, weakness","Visit PHC for Widal test. Antibiotic treatment required.","PHC","Same day","Yes","High"),
        ("Pneumonia","Cough with phlegm, chest pain, difficulty breathing","Visit CHC or hospital immediately for X-ray and antibiotics.","CHC/Hospital","Immediate","Yes","Critical"),
        ("Asthma","Wheezing, shortness of breath, chest tightness","Use inhaler if available. Avoid triggers. Visit PHC for prescription.","PHC","Same day","No","High"),
        ("Anaemia","Extreme tiredness, pale skin, dizziness","Iron-rich diet, iron supplements from PHC.","PHC","2-3 days","No","Medium"),
        ("Hypertension","Headache, dizziness, blurred vision","Reduce salt. Visit PHC for BP check and medication.","PHC","Same day","No","High"),
        ("Diabetes","Excessive thirst, frequent urination, fatigue","Visit PHC for blood sugar test. Lifestyle changes + medication.","PHC","2-3 days","No","High"),
        ("Snake Bite","Pain, swelling, bleeding at bite site","Immobilize limb, go to hospital IMMEDIATELY for anti-venom.","District Hospital","Immediate","Yes","Critical"),
        ("Dog Bite","Wound at bite site, risk of rabies","Clean wound, visit PHC immediately for rabies vaccination.","PHC","Immediate","Yes","Critical"),
        ("Jaundice","Yellow eyes/skin, dark urine, fatigue","Visit PHC for Hepatitis test. Rest, avoid fatty foods.","PHC","Same day","Yes","High"),
        ("Heart Attack","Chest pain, left arm pain, sweating","Call 108 immediately. Chew aspirin if available.","District Hospital","Immediate","Yes","Critical"),
        ("Stroke","Face drooping, arm weakness, speech difficulty","Call 112 immediately. Note time symptoms started.","District Hospital","Immediate","Yes","Critical"),
        ("Burns","Skin redness, blistering","Cool with running water 10 min. Do NOT use ice/oil.","PHC/Hospital","Immediate","Yes","Critical"),
        ("Dehydration","Dry mouth, dark urine, dizziness","ORS solution every 5 minutes.","PHC","Same day","No","High"),
        ("Malnutrition","Underweight, weak muscles, slow growth in children","Nutritional rehabilitation at Anganwadi/PHC.","PHC/NRC","2-3 days","No","High"),
    ]

    cursor.executemany("""
        INSERT INTO symptoms (disease,symptoms,first_aid_advice,recommended_facility,urgency_timeframe,needs_ambulance,severity)
        VALUES (?,?,?,?,?,?,?)
    """, symptoms)

    schemes = [
        ("Ayushman Bharat - PMJAY","Families in lowest 40% income group per SECC data","Cashless treatment up to Rs 5 lakhs/year per family","All empanelled hospitals","Call 14555 or visit hospital with Aadhaar","pmjay.gov.in"),
        ("Janani Suraksha Yojana (JSY)","Pregnant women from BPL families","Cash incentive Rs 1400 (rural) for institutional delivery","PHC, CHC, Government Hospitals","Register at nearest PHC during ANC check-up","nhm.gov.in"),
        ("JSSK","ALL pregnant women at government hospitals","Free delivery, medicines, diet, transport","Any Govt PHC/CHC/Hospital","Automatic — go to any government hospital","nhm.gov.in"),
        ("DOTS - Free TB Treatment","All TB patients in India","Free diagnosis and full course of medicines for 6 months","PHC, CHC, DOTS Centre","Visit nearest PHC with cough symptoms","nikshay.in"),
        ("RBSK","Children 0-18 years","Free screening and treatment for 30+ diseases","Schools and Anganwadi Centres","Automatic — teams visit schools and Anganwadis","nhm.gov.in"),
        ("PM National Dialysis Programme","BPL patients with Chronic Kidney Disease","Free dialysis at district hospitals","District Hospital","PMJAY card + BPL certificate at district hospital","nhm.gov.in"),
        ("PMMVY","Pregnant and lactating women - first child","Rs 5000 cash benefit in 3 instalments","Anganwadi Centre or PHC","Register at local Anganwadi or PHC","pmmvy.nic.in"),
        ("National Mental Health Programme","All citizens requiring mental health care","Free outpatient and inpatient mental health treatment","District Hospital DMHP Clinics","Visit district hospital psychiatry OPD","nhm.gov.in"),
    ]

    cursor.executemany("""
        INSERT INTO schemes (scheme_name,eligibility,benefit,where_available,how_to_apply,website)
        VALUES (?,?,?,?,?,?)
    """, schemes)

    emergency = [
        ("All India","Ambulance","108","Free 24x7 ambulance service"),
        ("All India","Ambulance Alternate","102","Government ambulance"),
        ("All India","Police","112","Emergency police response"),
        ("All India","Health Helpline","104","Free tele-health consultation"),
        ("All India","Women Helpline","1091","Women in distress"),
        ("All India","Child Helpline","1098","Children in distress"),
        ("All India","PMJAY Helpline","14555","Ayushman Bharat queries"),
        ("All India","Mental Health","9152987821","iCall free counselling"),
        ("All India","Poison Control","1800-116-117","Poison emergency toll-free"),
        ("All India","Disaster Management","1070","Natural disaster response"),
    ]

    cursor.executemany("""
        INSERT INTO emergency_contacts (state_ut,service,number,description)
        VALUES (?,?,?,?)
    """, emergency)

    conn.commit()
    conn.close()
    logger.info("Database seeded with initial data.")


def search_facilities(district):
    conn = get_connection()
    cursor = conn.cursor()
    results = cursor.execute(
        "SELECT * FROM facilities WHERE LOWER(district) LIKE LOWER(?) ORDER BY facility_type",
        (f"%{district}%",)
    ).fetchall()
    conn.close()
    return [dict(row) for row in results]


def get_all_schemes():
    conn = get_connection()
    cursor = conn.cursor()
    results = cursor.execute("SELECT * FROM schemes").fetchall()
    conn.close()
    return [dict(row) for row in results]


def get_emergency_contacts(state=None):
    conn = get_connection()
    cursor = conn.cursor()
    if state:
        results = cursor.execute(
            "SELECT * FROM emergency_contacts WHERE state_ut = 'All India' OR LOWER(state_ut) LIKE LOWER(?)",
            (f"%{state}%",)
        ).fetchall()
    else:
        results = cursor.execute(
            "SELECT * FROM emergency_contacts WHERE state_ut = 'All India'"
        ).fetchall()
    conn.close()
    return [dict(row) for row in results]


def get_symptom_info(disease):
    conn = get_connection()
    cursor = conn.cursor()
    result = cursor.execute(
        "SELECT * FROM symptoms WHERE LOWER(disease) LIKE LOWER(?)",
        (f"%{disease}%",)
    ).fetchone()
    conn.close()
    return dict(result) if result else None


if __name__ == "__main__":
    print("Initializing database...")
    init_db()
    print("Database ready at data/health.db")
    print(f"Facilities loaded: {len(search_facilities('Agra'))}")
