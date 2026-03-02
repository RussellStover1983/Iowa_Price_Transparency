"""Seed CPT lookup table with ~90 common procedure codes.

Run directly: python -m etl.load_cpt

Source reference: CMS Medicare Physician Fee Schedule public data.
Codes organized by clinical category with lay-term common names for FTS matching.
"""

import asyncio
import json
import os

import aiosqlite
from dotenv import load_dotenv

load_dotenv()

DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/iowa_transparency.db")

# fmt: off
CPT_CODES = [
    # --- Orthopedic ---
    ("27447", "Total knee replacement (arthroplasty)", "orthopedic",
     ["knee replacement", "total knee", "knee surgery", "TKA"]),
    ("27130", "Total hip replacement (arthroplasty)", "orthopedic",
     ["hip replacement", "total hip", "hip surgery", "THA"]),
    ("29881", "Arthroscopy knee with meniscectomy", "orthopedic",
     ["knee scope", "knee arthroscopy", "meniscus surgery", "torn meniscus"]),
    ("29827", "Arthroscopy shoulder with rotator cuff repair", "orthopedic",
     ["rotator cuff repair", "shoulder surgery", "shoulder scope"]),
    ("27236", "Open treatment of femoral fracture", "orthopedic",
     ["broken hip repair", "hip fracture surgery", "femur fracture repair"]),
    ("23472", "Reconstruction of shoulder joint", "orthopedic",
     ["shoulder replacement", "reverse shoulder replacement"]),
    ("22630", "Lumbar spinal fusion", "orthopedic",
     ["back fusion", "spinal fusion", "lumbar fusion", "back surgery"]),
    ("63047", "Lumbar laminectomy", "orthopedic",
     ["back surgery", "laminectomy", "spinal decompression"]),
    ("20610", "Arthrocentesis major joint", "orthopedic",
     ["joint injection", "knee injection", "cortisone shot"]),
    ("20680", "Removal of implant deep", "orthopedic",
     ["hardware removal", "plate removal", "screw removal"]),

    # --- Imaging / Radiology ---
    ("70553", "MRI brain with and without contrast", "imaging",
     ["brain MRI", "head MRI", "MRI of the brain"]),
    ("73721", "MRI lower extremity joint without contrast", "imaging",
     ["knee MRI", "ankle MRI", "leg MRI"]),
    ("73221", "MRI upper extremity joint without contrast", "imaging",
     ["shoulder MRI", "elbow MRI", "wrist MRI"]),
    ("74177", "CT abdomen and pelvis with contrast", "imaging",
     ["CT scan", "abdominal CT", "belly CT", "CT abdomen"]),
    ("71260", "CT chest with contrast", "imaging",
     ["chest CT", "lung CT", "CT of chest"]),
    ("77067", "Screening mammography bilateral", "imaging",
     ["mammogram", "breast screening", "screening mammogram"]),
    ("77061", "Digital breast tomosynthesis bilateral", "imaging",
     ["3D mammogram", "tomo mammogram", "breast tomosynthesis"]),
    ("76856", "Ultrasound pelvic complete", "imaging",
     ["pelvic ultrasound", "pelvic US"]),
    ("76700", "Ultrasound abdominal complete", "imaging",
     ["abdominal ultrasound", "belly ultrasound", "abdominal US"]),
    ("72148", "MRI lumbar spine without contrast", "imaging",
     ["back MRI", "lumbar MRI", "lower back MRI", "spine MRI"]),
    ("71046", "Chest X-ray 2 views", "imaging",
     ["chest X-ray", "CXR", "chest radiograph"]),
    ("73030", "Shoulder X-ray minimum 2 views", "imaging",
     ["shoulder X-ray"]),
    ("73560", "Knee X-ray 1 or 2 views", "imaging",
     ["knee X-ray"]),
    ("70551", "MRI brain without contrast", "imaging",
     ["brain MRI", "head MRI"]),

    # --- Cardiology ---
    ("93306", "Echocardiography transthoracic complete", "cardiology",
     ["echocardiogram", "echo", "heart ultrasound", "cardiac echo"]),
    ("93000", "Electrocardiogram 12-lead with interpretation", "cardiology",
     ["ECG", "EKG", "heart rhythm test", "electrocardiogram"]),
    ("93452", "Left heart catheterization", "cardiology",
     ["cardiac catheterization", "heart cath", "cardiac cath", "coronary angiogram"]),
    ("93458", "Catheter placement coronary angiography", "cardiology",
     ["coronary angiography", "heart angiogram"]),
    ("33533", "Coronary artery bypass single graft", "cardiology",
     ["CABG", "heart bypass", "bypass surgery", "coronary bypass"]),
    ("92928", "Percutaneous coronary stent placement", "cardiology",
     ["heart stent", "coronary stent", "PCI", "angioplasty with stent"]),
    ("33361", "Transcatheter aortic valve replacement", "cardiology",
     ["TAVR", "heart valve replacement", "aortic valve replacement"]),
    ("93015", "Cardiovascular stress test", "cardiology",
     ["stress test", "treadmill test", "exercise stress test"]),
    ("93350", "Stress echocardiography", "cardiology",
     ["stress echo", "exercise echo"]),

    # --- Gastroenterology ---
    ("45378", "Colonoscopy diagnostic", "gastroenterology",
     ["colonoscopy", "colon screening", "colon scope"]),
    ("45385", "Colonoscopy with removal of polyps", "gastroenterology",
     ["colonoscopy with polypectomy", "polyp removal", "colon polyp"]),
    ("43239", "Upper GI endoscopy with biopsy", "gastroenterology",
     ["EGD with biopsy", "upper endoscopy", "stomach scope with biopsy"]),
    ("43235", "Upper GI endoscopy diagnostic", "gastroenterology",
     ["EGD", "upper endoscopy", "stomach scope", "esophagoscopy"]),
    ("47562", "Laparoscopic cholecystectomy", "gastroenterology",
     ["gallbladder removal", "lap chole", "gallbladder surgery"]),
    ("44180", "Laparoscopic appendectomy", "gastroenterology",
     ["appendix removal", "appendectomy"]),
    ("49505", "Inguinal hernia repair", "gastroenterology",
     ["hernia repair", "groin hernia surgery", "inguinal hernia"]),

    # --- Primary Care / E&M ---
    ("99213", "Office visit established patient level 3", "primary_care",
     ["doctor visit", "office visit", "follow-up visit", "routine visit"]),
    ("99214", "Office visit established patient level 4", "primary_care",
     ["doctor visit detailed", "extended office visit"]),
    ("99215", "Office visit established patient level 5", "primary_care",
     ["complex office visit", "comprehensive visit"]),
    ("99203", "Office visit new patient level 3", "primary_care",
     ["new patient visit", "first visit"]),
    ("99204", "Office visit new patient level 4", "primary_care",
     ["new patient visit detailed", "new patient comprehensive"]),
    ("99205", "Office visit new patient level 5", "primary_care",
     ["new patient visit complex"]),
    ("99385", "Preventive visit new patient 18-39", "primary_care",
     ["annual physical", "wellness exam", "preventive exam", "new patient physical"]),
    ("99395", "Preventive visit established patient 18-39", "primary_care",
     ["annual physical", "wellness visit", "yearly checkup"]),
    ("99396", "Preventive visit established patient 40-64", "primary_care",
     ["annual physical", "wellness visit", "yearly checkup"]),

    # --- Emergency / Urgent ---
    ("99283", "Emergency department visit level 3", "emergency",
     ["ER visit", "emergency room", "ED visit"]),
    ("99284", "Emergency department visit level 4", "emergency",
     ["ER visit moderate", "emergency room moderate"]),
    ("99285", "Emergency department visit level 5", "emergency",
     ["ER visit severe", "emergency room critical"]),

    # --- OB/GYN ---
    ("59400", "Obstetric care including delivery vaginal", "ob_gyn",
     ["vaginal delivery", "childbirth", "having a baby", "labor and delivery"]),
    ("59510", "Cesarean delivery", "ob_gyn",
     ["C-section", "cesarean section", "surgical delivery"]),
    ("58661", "Laparoscopic removal of ovarian cyst", "ob_gyn",
     ["ovarian cyst removal", "ovarian cystectomy"]),
    ("58571", "Laparoscopic hysterectomy with removal of uterus >250g", "ob_gyn",
     ["hysterectomy", "uterus removal"]),
    ("58558", "Hysteroscopy with biopsy", "ob_gyn",
     ["uterine biopsy", "hysteroscopy"]),

    # --- Urology ---
    ("52000", "Cystoscopy", "urology",
     ["bladder scope", "cystoscopy", "bladder exam"]),
    ("55700", "Prostate biopsy", "urology",
     ["prostate biopsy", "prostate sampling"]),
    ("52601", "Transurethral resection of prostate (TURP)", "urology",
     ["TURP", "prostate surgery", "prostate resection"]),

    # --- ENT ---
    ("42820", "Tonsillectomy under age 12", "ent",
     ["tonsil removal", "tonsillectomy"]),
    ("42821", "Tonsillectomy age 12 or over", "ent",
     ["tonsil removal adult", "tonsillectomy adult"]),
    ("30520", "Septoplasty", "ent",
     ["deviated septum repair", "nasal septum surgery", "septoplasty"]),

    # --- General Surgery ---
    ("19301", "Partial mastectomy", "general_surgery",
     ["lumpectomy", "breast lump removal", "partial breast removal"]),
    ("19303", "Simple complete mastectomy", "general_surgery",
     ["mastectomy", "breast removal"]),
    ("38571", "Laparoscopic lymph node biopsy", "general_surgery",
     ["lymph node biopsy", "lymph node sampling"]),
    ("10060", "Incision and drainage of abscess simple", "general_surgery",
     ["abscess drainage", "drain abscess", "I&D"]),
    ("11042", "Debridement subcutaneous tissue", "general_surgery",
     ["wound debridement", "wound cleaning"]),

    # --- Ophthalmology ---
    ("66984", "Cataract surgery with IOL insertion", "ophthalmology",
     ["cataract surgery", "cataract removal", "lens replacement"]),
    ("67028", "Intravitreal injection", "ophthalmology",
     ["eye injection", "retinal injection", "macular degeneration injection"]),

    # --- Pain Management / Anesthesia ---
    ("62322", "Lumbar epidural injection", "pain_management",
     ["epidural", "back injection", "lumbar epidural", "spinal injection"]),
    ("64483", "Transforaminal epidural injection lumbar", "pain_management",
     ["nerve block", "back nerve injection", "epidural steroid injection"]),
    ("27096", "Sacroiliac joint injection", "pain_management",
     ["SI joint injection", "sacroiliac injection"]),

    # --- Neurology ---
    ("95819", "EEG with sleep", "neurology",
     ["EEG", "brain wave test", "electroencephalogram"]),
    ("95907", "Nerve conduction study 1-2 nerves", "neurology",
     ["nerve conduction study", "NCS", "nerve test"]),
    ("95886", "Needle EMG complete", "neurology",
     ["EMG", "electromyography", "muscle test"]),

    # --- Pulmonology ---
    ("94010", "Spirometry", "pulmonology",
     ["breathing test", "lung function test", "spirometry", "PFT"]),
    ("94060", "Bronchospasm evaluation with spirometry", "pulmonology",
     ["bronchial challenge test", "methacholine challenge"]),
    ("31622", "Diagnostic bronchoscopy", "pulmonology",
     ["bronchoscopy", "lung scope"]),

    # --- Dermatology ---
    ("11102", "Tangential biopsy of skin single lesion", "dermatology",
     ["skin biopsy", "mole biopsy", "lesion biopsy"]),
    ("17000", "Destruction of premalignant lesion first", "dermatology",
     ["skin lesion removal", "precancer removal", "actinic keratosis treatment"]),
    ("17110", "Destruction of warts up to 14", "dermatology",
     ["wart removal", "wart treatment", "wart destruction"]),

    # --- Lab / Pathology (commonly bundled) ---
    ("80053", "Comprehensive metabolic panel", "lab",
     ["CMP", "blood panel", "metabolic panel", "blood chemistry"]),
    ("85025", "Complete blood count with differential", "lab",
     ["CBC", "blood count", "complete blood count"]),
    ("81001", "Urinalysis automated with microscopy", "lab",
     ["urine test", "urinalysis", "UA"]),
    ("36415", "Venipuncture routine", "lab",
     ["blood draw", "venipuncture", "blood collection"]),
    ("84443", "Thyroid stimulating hormone (TSH)", "lab",
     ["TSH", "thyroid test", "thyroid blood test"]),
    ("83036", "Hemoglobin A1c", "lab",
     ["A1c", "HbA1c", "diabetes test", "hemoglobin A1c"]),
]
# fmt: on

# Iowa-adjusted MPFS rates (Locality 00, CY 2025) and OPPS APC rates (CY 2025)
# Format: CPT -> (mpfs_facility_rate, mpfs_non_facility_rate, opps_rate)
# mpfs = physician payment; opps = hospital facility payment
# None means not separately payable under that system
MEDICARE_RATES = {
    # Orthopedic
    "27447": (1144.67, 1144.67, 12866.82),
    "27130": (1146.00, 1146.00, 12866.82),
    "29881": (490.70, 490.70, 3244.61),
    "29827": (958.10, 958.10, 7143.73),
    "27236": (1067.89, 1067.89, None),  # Inpatient only
    "23472": (1290.01, 1290.01, 18390.05),
    "22630": (1356.25, 1356.25, 18390.05),
    "63047": (971.09, 971.09, 7143.73),
    "20610": (40.47, 58.19, 295.19),
    "20680": (379.59, 533.75, 2862.05),
    # Imaging
    "70553": (290.84, 290.84, 357.13),
    "73721": (185.28, 185.28, 241.72),
    "73221": (185.57, 185.57, 241.72),
    "74177": (274.75, 274.75, 357.13),
    "71260": (152.26, 152.26, 178.02),
    "77067": (115.10, 115.10, None),
    "77061": (None, None, None),  # Invalid code
    "76856": (93.94, 93.94, 106.34),
    "76700": (103.58, 103.58, 106.34),
    "72148": (174.72, 174.72, 241.72),
    "71046": (30.15, 30.15, 88.05),
    "73030": (30.93, 30.93, 88.05),
    "73560": (30.28, 30.28, 88.05),
    "70551": (179.44, 179.44, 241.72),
    # Cardiology
    "93306": (174.66, 174.66, 548.30),
    "93000": (12.88, 12.88, None),
    "93452": (757.63, 757.63, 3216.41),
    "93458": (878.86, 878.86, 3216.41),
    "33533": (1610.12, 1610.12, None),  # Inpatient only
    "92928": (502.96, 502.96, 11340.57),
    "33361": (1035.38, 1035.38, None),  # Inpatient only
    "93015": (66.20, 66.20, None),
    "93350": (164.77, 164.77, 548.30),
    # Gastroenterology
    "45378": (164.67, 301.99, 911.71),
    "45385": (226.39, 402.11, 1179.08),
    "43239": (123.53, 328.49, 937.56),
    "43235": (109.69, 254.11, 937.56),
    "47562": (583.24, 583.24, 5834.36),
    "44180": (809.25, 809.25, 5834.36),
    "49505": (463.79, 463.79, 3529.06),
    # Primary Care
    "99213": (60.36, 83.40, None),
    "99214": (88.83, 117.48, None),
    "99215": (131.48, 165.14, None),
    "99203": (74.50, 101.67, None),
    "99204": (121.61, 152.92, None),
    "99205": (165.20, 201.82, None),
    "99385": (86.18, 118.66, None),
    "99395": (78.76, 107.11, None),
    "99396": (85.53, 113.88, None),
    # Emergency
    "99283": (64.46, 64.46, 276.89),
    "99284": (109.75, 109.75, 425.82),
    "99285": (159.07, 159.07, 613.10),
    # OB/GYN
    "59400": (2106.28, 2106.28, None),
    "59510": (2322.97, 2322.97, None),
    "58661": (584.08, 584.08, 5834.36),
    "58571": (814.08, 814.08, 10411.22),
    "58558": (206.22, 1103.41, 3179.53),
    # Urology
    "52000": (72.23, 196.27, 667.47),
    "55700": (116.75, 215.69, 2048.51),
    "52601": (659.16, 659.16, 5083.62),
    # ENT
    "42820": (265.42, 265.42, 5915.66),
    "42821": (276.74, 276.74, 3243.07),
    "30520": (601.53, 601.53, 3243.07),
    # General Surgery
    "19301": (582.43, 582.43, 3829.28),
    "19303": (845.55, 845.55, 6521.46),
    "38571": (598.16, 598.16, 10411.22),
    "10060": (97.20, 114.92, 198.70),
    "11042": (54.67, 115.21, 399.53),
    # Ophthalmology
    "66984": (488.93, 488.93, 2280.73),
    "67028": (83.20, 101.81, 331.69),
    # Pain Management
    "62322": (71.11, 118.65, 890.29),
    "64483": (101.33, 218.57, 890.29),
    "27096": (75.78, 147.25, None),
    # Neurology
    "95819": (401.28, 401.28, 311.40),
    "95907": (80.34, 80.34, 156.46),
    "95886": (84.67, 84.67, None),
    # Pulmonology
    "94010": (24.40, 24.40, 156.46),
    "94060": (34.88, 34.88, 311.40),
    "31622": (118.02, 224.93, 1724.47),
    # Dermatology
    "11102": (34.05, 88.39, 198.70),
    "17000": (50.00, 61.52, 198.70),
    "17110": (62.81, 101.49, 198.70),
    # Lab (CLFS rates - no geographic variation)
    "80053": (10.56, 10.56, None),
    "85025": (7.77, 7.77, None),
    "81001": (3.49, 3.49, None),
    "36415": (9.09, 9.09, None),
    "84443": (16.80, 16.80, None),
    "83036": (11.41, 11.41, None),
}


async def load_cpt_codes(db_path: str | None = None):
    """Insert CPT codes into cpt_lookup and rebuild FTS index."""
    path = db_path or DATABASE_PATH

    db = await aiosqlite.connect(path)
    try:
        await db.execute("PRAGMA foreign_keys=ON")

        for code, description, category, common_names in CPT_CODES:
            names_json = json.dumps(common_names)
            medicare = MEDICARE_RATES.get(code, (None, None, None))
            await db.execute(
                "INSERT OR REPLACE INTO cpt_lookup "
                "(code, description, category, common_names, "
                " medicare_facility_rate, medicare_professional_rate, medicare_opps_rate) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (code, description, category, names_json,
                 medicare[0], medicare[1], medicare[2]),
            )

        # Rebuild FTS index
        await db.execute("INSERT INTO cpt_fts(cpt_fts) VALUES ('rebuild')")
        await db.commit()

        cursor = await db.execute("SELECT COUNT(*) FROM cpt_lookup")
        count = (await cursor.fetchone())[0]
        print(f"Loaded {count} CPT codes into cpt_lookup + FTS index")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(load_cpt_codes())
