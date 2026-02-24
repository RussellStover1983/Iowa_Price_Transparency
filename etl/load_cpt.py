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


async def load_cpt_codes(db_path: str | None = None):
    """Insert CPT codes into cpt_lookup and rebuild FTS index."""
    path = db_path or DATABASE_PATH

    db = await aiosqlite.connect(path)
    try:
        await db.execute("PRAGMA foreign_keys=ON")

        for code, description, category, common_names in CPT_CODES:
            names_json = json.dumps(common_names)
            await db.execute(
                "INSERT OR REPLACE INTO cpt_lookup (code, description, category, common_names) "
                "VALUES (?, ?, ?, ?)",
                (code, description, category, names_json),
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
