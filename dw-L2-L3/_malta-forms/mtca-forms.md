# Malta Tax Forms Comprehensive Catalogue (2018-2025)
## MTCA Digital Transformation: ORS Schema Design Reference

Malta's tax administration operates through the **Malta Tax and Customs Administration (MTCA)** and **Commissioner for Revenue (CFR)**, managing over **100 distinct tax forms** across direct taxes, indirect taxes, customs duties, and employer obligations. This catalogue documents all publicly available forms for the period 2018-2025, structured to inform database schema design and legacy Informix data migration planning.

---

## 1. Master Form Registry

### 1.1 Direct Taxes — Individual Income Tax

| Form Code | Official Name | Tax Type | Taxpayer Type | Filing Frequency | Submission Method | Validity Period | Legislative Basis |
|-----------|---------------|----------|---------------|------------------|-------------------|-----------------|-------------------|
| **e-Return** | Individual Income Tax Return | Income Tax | Individual | Annual | Online (myTax portal, e-ID required) | 2018-2025 | ITA Cap. 123, ITMA Cap. 372 |
| **TA22** | Part-Time Self-Employment Declaration | Income Tax | Individual (PT self-employed) | Annual (April 30) | Both (paper/online) | 2018-2025 | ITA Cap. 123 |
| **TA23** | Part-Time Employment Under-Deducted Tax Payment | Income Tax | Individual | Annual | Both | 2018-2025 | ITA Cap. 123 |
| **TA24** | Residential/Commercial Rental Income Declaration | Income Tax | Individual | Annual | Both | 2014-2025 | ITA Cap. 123 |
| **PT1** | Provisional Tax Payment Form | Income Tax | Self-employed | 3x/year (Apr, Aug, Dec) | Sent by CFR; payment online | 2018-2025 | ITMA Cap. 372 |
| **AF** | Non-Filer Correction Form | Income Tax | Individual | Event-driven | Paper | 2018-2025 | ITMA Cap. 372 |
| **AF1** | Filed Return Adjustment Form | Income Tax | Individual | Event-driven | Paper | 2018-2025 | ITMA Cap. 372 |

**Key Data Fields (Individual Returns):** Personal ID (TIN), income categories (emoluments, business, rental, investment), deductions (school fees, sports, transport), capital gains, FSS deductions, provisional tax payments, marital/parent status.

### 1.2 Tax Credit Attachment Forms (RA Series)

| Form Code | Official Name | Purpose | Validity |
|-----------|---------------|---------|----------|
| RA6 | Student Hosting Net Profit | Deduction for student accommodation income | 2018-2025 |
| RA7 | Women Returning to Employment Credit | Tax credit (5+ years absence) | 2008-2025 |
| RA9 | Women Returning After Maternity Credit | Post-childbirth/adoption credit | 2018-2025 |
| RA10 | Qualifications Tax Credit | Malta Enterprise-approved qualifications | 2018-2025 |
| RA15 | MicroInvest Tax Credit | SME investment credit | 2018-2025 |
| RA17 | Highly Qualified Persons (HQP) | Article 58(21) beneficial tax treatment | 2018-2025 |
| RA21 | Aviation Employment | 15% preferential rate aviation sector | 2018-2025 |
| RA24 | R&D Tax Credit | Research &amp; development expenditure | 2018-2025 |
| RA26 | Business Development &amp; Continuity | COVID-era business credits | 2020-2025 |
| RA29 | Maritime/Offshore Oil &amp; Gas Employment | Beneficial rates for maritime sector | 2018-2025 |
| RA30 | Higher Educational Qualification Credit | Higher education credits | 2018-2025 |

### 1.3 Direct Taxes — Corporate/Business

| Form Code | Official Name | Tax Type | Entity Type | Filing Frequency | Submission Method | Validity | Legislative Basis |
|-----------|---------------|----------|-------------|------------------|-------------------|----------|-------------------|
| **e-CO** | Electronic Company Income Tax Return | Corporate Tax | Companies | Annual (9 months post year-end) | **Mandatory electronic** | 2002-2025 | ITA Cap. 123 |
| **TIFD** | Tax Index of Financial Data | Corporate Tax | Companies | Annual (with e-CO) | Electronic spreadsheet | 2002-2025 | ITMA Cap. 372 |
| **Fiscal Unit Return** | Fiscal Unit Supplemental Document | Corporate Tax | Group companies (95%+) | Annual | Electronic | 2019-2025 | LN 110/2019 |

**Corporate Tax Return Attachments (TRA Series):**

| TRA Code | Name | Purpose | Key Updates |
|----------|------|---------|-------------|
| TRA 19 | Group Loss Relief - Surrendering | Surrender trading losses to group | Used with TRA 20 |
| TRA 20 | Group Loss Relief - Claimant | Claim surrendered group losses | File by return deadline |
| TRA 53 | Property Transfer Tax | Capital gains on property | Updated for reduced FWT rates |
| TRA 61 | Final Tax Account Allocation | FTA reserves movement | 2018-2025 |
| TRA 63 | Tax Account Allocations | 5-account allocation (FTA, MTA, FIA, IPA, UA) | Updated YA 2024 for NID reallocation |
| TRA 82 | Wages to Non-Residents | EU resident TIN mandatory | Updated YA 2023 |
| TRA 89 | Article 27G Elections | Participation exemption elections | 2018-2025 |
| TRA 100 | Notional Interest Deduction (NID) | Claim NID on risk capital | Updated for pro-rata calculations |
| TRA 100A | NID Supplementary | Excess NID carried forward | 2018-2025 |
| TRA 107 | Double Taxation Relief | Foreign tax credit claims | Updated YA 2024 |
| TRA 111 | Interest Limitation Rules (ATAD) | Unused interest capacity surrender | ATAD implementation |
| TRA 125 | Group Deductions Rules | Capital allowance surrender | Extended YA 2023 |
| TRA 131 | Rent Subsidy Regulations | Tax credits S.L.463.33 | **NEW YA 2025** |
| TRA 132 | Green Mobility Regulations | Tax credits S.L.463.56 | **NEW YA 2025** |
| TRA 133 | Business Development 2024 | Tax credits S.L.463.57 | **NEW YA 2025** |
| TRA 134 | Donations Tax Credit | Charitable donations relief | **NEW YA 2025** |

### 1.4 Capital Gains Tax — Property and Share Transfers

| Schedule | Official Name | Purpose | Filing Frequency | Submission |
|----------|---------------|---------|------------------|------------|
| Schedule B | Notice of Transfer of Right | Property transfer notification (Art 5(1) ITA) | Event-driven | Notary submits |
| Schedule B1 | Notice of Transfer of Promise of Right | Promise of sale notification | Event-driven | Notary submits |
| Schedule C | Transfer of Shares - Controlling Interest | Share transfer ≥25% | Event-driven | Accountant/CFR |
| Schedule D | Transfer of Shares - Non-Controlling | Share transfer &lt;25% | Event-driven | Accountant/CFR |
| Schedule E | Transfer of Shares - Exempt from CGT | Exempt share transfers | Event-driven | Accountant/CFR |
| Schedule F | Changes in Share Capital/Voting Rights | Capital/voting changes notification | Event-driven | Company secretary |
| Eighth Schedule | Physical Attributes of Immovable Property | Property details for transfers | Event-driven | With Schedule B |

### 1.5 VAT and Indirect Taxes

| Form Code | Official Name | Tax Type | Taxpayer Type | Filing Frequency | Submission Method | Validity |
|-----------|---------------|----------|---------------|------------------|-------------------|----------|
| **VAT Online Form 1** | VAT Registration (Article 10) | VAT | Taxable persons | One-time | Electronic (e-ID) | 2018-2025 |
| **VAT Online Form 2** | VAT Registration (Article 11) | VAT | Small undertakings (&lt;€30K) | One-time | Electronic | 2018-2025 |
| **VAT Online Form 3/4** | Article 12 Registration | VAT | Intra-Community acquirers | One-time | Electronic | 2018-2025 |
| **VAT Return (Art 10)** | Periodic VAT Return | VAT | Art 10 registrants | Quarterly/Monthly | **Mandatory electronic** (Feb 2023+) | 2018-2025 |
| **VAT Return (Art 11)** | Annual VAT Declaration | VAT | Art 11 registrants | Annual (Feb 15) | Electronic | 2018-2025 |
| **VAT Form 004/2010** | Reverse Charge Notification | VAT | Art 10/12 registrants | Event-driven | Paper/Online | 2018-2025 |
| **Recapitulative Statement** | EC Sales List (VIES) | VAT | Art 10 with EU supplies | Monthly/Quarterly | **Mandatory electronic** | 2009-2025 |
| **Intrastat Declaration** | Arrivals/Dispatches | VAT/Statistics | Traders &gt;€700/year | Monthly | **Electronic only** (NSO) | 2018-2025 |
| **Union OSS Return** | One Stop Shop Return | VAT | B2C cross-border suppliers | Quarterly | Electronic (CFR) | July 2021-2025 |
| **Non-Union OSS Return** | OSS for Non-EU Suppliers | VAT | Non-EU B2C suppliers | Quarterly | Electronic | July 2021-2025 |
| **IOSS Return** | Import OSS Return | VAT | Low-value importers (&lt;€150) | Monthly | Electronic | July 2021-2025 |

**VAT Refund Forms:**

| Form | Purpose | Who Files | Deadline |
|------|---------|-----------|----------|
| Tourist VAT Refund | Non-EU travelers refund | Travelers | At customs exit |
| 8th Directive Refund | EU business refund (not registered in Malta) | EU businesses | Sep 30 following year |
| 13th Directive Refund | Non-EU business refund | Non-EU businesses | Jun 30 following year |

### 1.6 Customs and Excise Duties

| Form Code | Official Name | Purpose | Filing Method | Validity |
|-----------|---------------|---------|---------------|----------|
| **SAD** | Single Administrative Document | Import/Export/Transit declarations | Electronic (NIS/NES) | 2018-2025 |
| **ENS** | Entry Summary Declaration | Pre-arrival safety/security | **Mandatory electronic** (ICS2) | 2021-2025 |
| **EXS** | Exit Summary Declaration | Pre-departure safety/security | Electronic (NES) | 2018-2025 |
| **T1/T2** | Transit Declarations | External/Internal Union transit | **Mandatory electronic** (NCTS-P5) | 2018-2025 |
| **T2L/T2LF** | Proof of Union Status | Union status certification | Electronic | 2018-2025 |
| **EORI Application** | Economic Operator Registration | Customs ID registration | Electronic (e-Forms) | 2009-2025 |
| **AEO Application** | Authorized Economic Operator | Trusted trader certification | Electronic (EU CDS) | 2018-2025 |
| **BTI Application** | Binding Tariff Information | Tariff classification ruling | **Mandatory electronic** (EBTI) | 2019-2025 |
| **EUR.1** | Movement Certificate | Preferential origin proof | Paper (stamped) | 2018-2025 |
| **Form A** | GSP Certificate of Origin | GSP beneficiary origin | Paper (transitioning to REX) | 2018-2025 |

**Excise Duty Forms:**

| Form Code | Official Name | Purpose | Submission Method | Legal Basis |
|-----------|---------------|---------|-------------------|-------------|
| **e-AD** | Electronic Administrative Document | Duty-suspended EU movement | **Mandatory electronic** (EMCS) | Directive 2020/262 |
| **e-SAD** | Electronic Simplified Administrative Document | Duty-paid EU movement | **Mandatory electronic** (EMCS) | Directive 2020/262 |
| **IAAD** | Internal Administrative Accompanying Document | Domestic duty-suspended movement | Paper (download) | Excise Duty Act Cap. 382 |
| **AWK Application** | Authorized Warehouse Keeper Application | Tax warehouse authorization | Electronic (e-Forms) | Cap. 382 |
| **ERC/ECC Application** | Registered/Certified Consignee | Receive excise goods from EU | Electronic (e-Forms) | Cap. 382 |

### 1.7 Stamp Duties and Document Transfers

| Form Code | Official Name | Tax Type | Filing Frequency | Submission |
|-----------|---------------|----------|------------------|------------|
| **DDT1** | Notice of Transfer (Property) | Stamp Duty | Per transaction | Notary to Capital Transfer Duty Dept |
| **Schedule 8** | Physical Attributes - Residential Property | Stamp Duty | With DDT1 | Paper |
| **Promise of Sale** | Provisional 1% Duty Registration | Stamp Duty | Within 21 days of signing | Notary |
| **Fifth Schedule** | Share Capital/Voting Rights Alteration | Duty on Documents | Event-driven | Paper |
| **Sixth Schedule** | Company Share Transfer | Duty on Documents | Event-driven | Paper |
| **Seventh Schedule** | Share Transfer Exempt from Duty | Duty on Documents | Event-driven | Paper |

### 1.8 Environmental Taxes

| Form Code | Official Name | Tax Type | Filing Frequency | Submission Method | Validity |
|-----------|---------------|----------|------------------|-------------------|----------|
| **ECO Products Return** | Eco-Contribution on Products | Environmental | Quarterly (with VAT) | Electronic (VAT portal) | 2018-2025 |
| **ECO Accommodation Return** | Environmental Contribution on Accommodation | Environmental | Quarterly (Feb, May, Aug, Nov 15) | Electronic (e-ID) | June 2016-2025 |
| **BCRS Registration** | Beverage Container Refund Scheme | Environmental | One-time | Electronic (BCRS Malta) | Nov 2022-2025 |
| **VEH 007** | Motor Vehicle Registration Tax Exemption (TORE) | Vehicle Tax | Event-driven | Paper/Online | 2018-2025 |

### 1.9 Gaming Tax

| Form | Purpose | Filing Frequency | Submission | Authority |
|------|---------|------------------|------------|-----------|
| **Gaming Tax Return** | Gaming revenue declaration | Monthly (by 20th) | Electronic (LRMS portal) | MGA |
| **Industry Performance Return** | Operational data | Bi-annual (Feb 28) | Electronic | MGA |
| **Compliance Contribution Return** | License compliance fees | Monthly (by 20th) | Electronic | MGA |

### 1.10 Employer Obligations (FSS System)

| Form Code | Official Name | Purpose | Filing Frequency | Submission Method | Mandatory Electronic |
|-----------|---------------|---------|------------------|-------------------|---------------------|
| **FS3** | Payee Statement of Earnings | Annual employee tax summary | Annual (Feb 15) | Electronic/Paper | **Yes (10+ employees)** |
| **FS4** | Payee Status Declaration | New employee tax status | Within 7 days of hire | Electronic preferred | Recommended |
| **FS5** | Payer's Monthly Payment Advice | Monthly FSS/SSC remittance | Monthly (last working day) | **Mandatory electronic** | **Yes (all employers)** |
| **FS7** | Annual Reconciliation Statement | Annual FSS/SSC reconciliation | Annual (Feb 15) | Electronic/Paper | **Yes (10+ employees)** |
| **FB1** | Fringe Benefits Personal History | Fringe benefits documentation | Annual | Paper | No |

---

## 2. Taxpayer Identifier Formats

Understanding identifier formats is critical for ORS schema design and cross-system data integration.

| Identifier | Format | Structure | Check Digit | Example |
|------------|--------|-----------|-------------|---------|
| **Individual TIN (Maltese)** | (0000)999L | 7 digits + 1 letter (M,G,A,P,L,H,B,Z) | Last letter | 1234567M |
| **Individual TIN (Non-Maltese)** | 999999999 | 9 digits | None specified | 123456789 |
| **Company TIN** | 999999999 | 9 digits (auto-generated by MBR) | None specified | 987654321 |
| **VAT Number** | MT + 8 digits | Country prefix + 8 numeric | None specified | MT12345678 |
| **EORI Number** | MT + VAT Number | Same as VAT number | None | MT12345678 |
| **Company ROC Number** | C + number | "C" prefix + sequential | None | C12345 |

---

## 3. Form Evolution Timeline (2018-2025)

### 2018
- **NID Rules introduced** (S.L.123.176) — TRA 100/100A forms for Notional Interest Deduction claims
- **e-Invoicing transposition** — LN 403 &amp; 404 implementing EU Directive 2014/55/EU

### 2019
- **DAC6 implementation** — LN 342/2019 introducing reportable cross-border arrangement forms (XML schema)
- **Fiscal Unit Regulations** — LN 110/2019 enabling tax consolidation with new supplemental documents
- **BTI electronic mandatory** — All Binding Tariff Information applications via EU EBTI system (Oct 2019)

### 2020
- **RA26 introduced** — Business Development &amp; Continuity Scheme (COVID response)
- **NID guidelines updated** — Pro-rata calculations for non-12 month accounting periods
- **FSS real-time processing** — Electronic FSS system enhancements

### 2021
- **OSS replaces MOSS** (July 1) — New Union OSS, Non-Union OSS, and IOSS quarterly returns replace Mini One Stop Shop
- **€10,000 EU threshold** — Replaces national distance selling thresholds
- **€22 import exemption abolished** — All imports now subject to VAT
- **Article 11 thresholds updated** — Entry €30,000, Exit €24,000
- **ICS2 Release 1** (October) — Entry Summary Declarations for postal operators
- **CRS/CbC XML v2.0** — Mandatory adoption of OECD schema version 2.0

### 2022
- **BCRS launched** (November) — Beverage Container Refund Scheme producer registration
- **Transfer Pricing Rules** — LN 284/2022 publishing Malta's TP framework
- **Work-Life Balance Fund** — LN 201/2022 expanding maternity fund scope
- **NIS upgrade completed** — Q4 2022 National Import System modernization

### 2023
- **VAT e-filing mandatory** (February 15) — All Article 10 VAT returns mandatory electronic
- **DAC7 implementation** — LN 8/2023 for digital platform operator reporting
- **TRA 82 updated** — EU resident director TIN now mandatory
- **TRA 125 extended** — Now includes unabsorbed capital allowances
- **ICS2 Release 2** (March) — Air cargo and express carriers included
- **Recapitulative Statement penalties** — Increased to €50/month, capped €600

### 2024
- **Transfer Pricing effective** — Full applicability from January 1, 2024
- **TRA 63, 100, 100A, 107 updated** — Various technical amendments
- **TRA 130 introduced** — New tax return attachment
- **SME VAT Scheme** — €100,000 EU-wide threshold preparation

### 2025
- **Transfer pricing question added** — Page 2 of company e-Return
- **TRA 131-134 introduced** — Rent Subsidy, Green Mobility, Business Development 2024, Donations Tax Credit
- **Elective Tax (Pillar 2)** — OECD Global Minimum Tax forms preparation
- **ViDA preparation** — Real-time reporting infrastructure development

---

## 4. Data Field Mapping — Standardized Field Catalogue

### Core Entity Fields (All Forms)

| Field Name | Data Type | Max Length | Format/Validation | Used In |
|------------|-----------|------------|-------------------|---------|
| `taxpayer_tin` | VARCHAR | 9 | Alphanumeric, last char letter (Maltese) or all numeric | All forms |
| `vat_number` | VARCHAR | 10 | "MT" + 8 digits | VAT, Customs |
| `eori_number` | VARCHAR | 10 | Same as VAT number | Customs |
| `company_roc` | VARCHAR | 10 | "C" + sequential number | Corporate |
| `accounting_period_start` | DATE | 10 | YYYY-MM-DD | Corporate, VAT |
| `accounting_period_end` | DATE | 10 | YYYY-MM-DD | Corporate, VAT |
| `year_of_assessment` | INT | 4 | YYYY | All annual forms |

### Income Tax Fields

| Field Name | Data Type | Max Length | Description | Forms |
|------------|-----------|------------|-------------|-------|
| `gross_emoluments` | DECIMAL | 15,2 | Employment income before deductions | Individual Return, FS3 |
| `business_income` | DECIMAL | 15,2 | Self-employment/business profits | Individual Return, TA22 |
| `rental_income_gross` | DECIMAL | 15,2 | Gross rental receipts | TA24 |
| `investment_income` | DECIMAL | 15,2 | Dividends, interest, royalties | Individual Return |
| `capital_gains` | DECIMAL | 15,2 | Property/share disposal gains | Schedule B-F |
| `fss_tax_deducted` | DECIMAL | 15,2 | Tax withheld at source | FS3, FS5 |
| `ssc_employee` | DECIMAL | 15,2 | Employee SSC contribution | FS3, FS5 |
| `ssc_employer` | DECIMAL | 15,2 | Employer SSC contribution | FS5 |
| `maternity_fund` | DECIMAL | 15,2 | 0.3% maternity/WLB contribution | FS5 |
| `provisional_tax_paid` | DECIMAL | 15,2 | PT installments paid | PT1, Individual Return |

### Corporate Tax Fields

| Field Name | Data Type | Max Length | Description | Forms |
|------------|-----------|------------|-------------|-------|
| `trading_income` | DECIMAL | 15,2 | Trading profits | e-CO, TRA 63 |
| `investment_income_corp` | DECIMAL | 15,2 | Passive investment income | e-CO, TRA 63 |
| `fta_allocation` | DECIMAL | 15,2 | Final Tax Account allocation | TRA 61, TRA 63 |
| `mta_allocation` | DECIMAL | 15,2 | Maltese Taxed Account allocation | TRA 63 |
| `fia_allocation` | DECIMAL | 15,2 | Foreign Income Account allocation | TRA 63 |
| `ipa_allocation` | DECIMAL | 15,2 | Immovable Property Account | TRA 63 |
| `ua_allocation` | DECIMAL | 15,2 | Untaxed Account allocation | TRA 63 |
| `nid_amount` | DECIMAL | 15,2 | Notional Interest Deduction claimed | TRA 100 |
| `group_loss_surrendered` | DECIMAL | 15,2 | Losses surrendered to group | TRA 19 |
| `group_loss_claimed` | DECIMAL | 15,2 | Losses claimed from group | TRA 20 |
| `dtr_claimed` | DECIMAL | 15,2 | Double taxation relief | TRA 107 |

### VAT Fields

| Field Name | Data Type | Max Length | Description | Forms |
|------------|-----------|------------|-------------|-------|
| `output_vat` | DECIMAL | 15,2 | VAT charged on supplies | VAT Return |
| `input_vat` | DECIMAL | 15,2 | VAT recoverable on purchases | VAT Return |
| `net_vat_payable` | DECIMAL | 15,2 | Output minus input VAT | VAT Return |
| `taxable_supplies` | DECIMAL | 15,2 | Total taxable supplies value | VAT Return |
| `exempt_supplies` | DECIMAL | 15,2 | VAT-exempt supplies value | VAT Return |
| `intra_community_supplies` | DECIMAL | 15,2 | EU B2B supplies (zero-rated) | Recapitulative Statement |
| `intra_community_acquisitions` | DECIMAL | 15,2 | EU B2B purchases | VAT Return, Intrastat |
| `customer_vat_number` | VARCHAR | 14 | EU customer VAT ID | Recapitulative Statement |
| `cn_code` | VARCHAR | 8 | Combined Nomenclature code | Intrastat |
| `statistical_value` | DECIMAL | 15,2 | Statistical value (CIF/FOB) | Intrastat |

### Customs Fields

| Field Name | Data Type | Max Length | Description | Forms |
|------------|-----------|------------|-------------|-------|
| `mrn` | VARCHAR | 18 | Movement Reference Number | SAD, T1/T2, e-AD |
| `arc` | VARCHAR | 21 | Administrative Reference Code | e-AD, e-SAD |
| `declaration_type` | VARCHAR | 4 | H1-H7 declaration category | SAD |
| `consignor_eori` | VARCHAR | 17 | Sender EORI (Box 2) | SAD |
| `consignee_eori` | VARCHAR | 17 | Recipient EORI (Box 8) | SAD |
| `declarant_eori` | VARCHAR | 17 | Declarant EORI (Box 14) | SAD |
| `customs_value` | DECIMAL | 15,2 | Transaction value for duty | SAD |
| `duty_amount` | DECIMAL | 15,2 | Customs duty payable | SAD |
| `excise_amount` | DECIMAL | 15,2 | Excise duty payable | e-AD, SAD |
| `country_of_origin` | VARCHAR | 2 | ISO country code | SAD, EUR.1 |
| `preference_code` | VARCHAR | 3 | Preferential treatment code | SAD |

---

## 5. Form Dependencies and Relationships

### Individual Tax Filing Chain
```
FS4 (New Employee) → FS5 (Monthly) → FS3 (Annual) → FS7 (Reconciliation)
                                           ↓
                                  Individual e-Return
                                           ↓
                              RA Forms (if claiming credits)
```

### Corporate Tax Filing Chain
```
Company e-CO Return
       ↓
    TIFD (Financial Data Attachment)
       ↓
    TRA Forms (based on circumstances):
       ├── TRA 63 (Tax Account Allocations) — MANDATORY
       ├── TRA 100/100A (if claiming NID)
       ├── TRA 19/20 (if group relief)
       ├── TRA 107 (if DTR claimed)
       └── TRA 131-134 (if claiming specific credits)
       ↓
Shareholder Refund Registration → Refund Claim (6/7ths, 5/7ths, etc.)
```

### Property Transfer Chain
```
Promise of Sale Signed
       ↓
DDT1 (within 21 days) + Schedule 8 + 1% provisional duty
       ↓
Final Deed Execution
       ↓
Schedule B (Transfer Notification) + Eighth Schedule
       ↓
Balance Duty Payment (4% of 5% total)
       ↓
Capital Gains Tax (8%/10% FWT withheld by notary)
```

### VAT Compliance Chain
```
VAT Form 1/2/3 (Registration)
       ↓
VAT Return (Quarterly/Monthly)
       ↓
    ├── Recapitulative Statement (if EU supplies)
    ├── Intrastat Declaration (if goods movement >€700)
    └── OSS Return (if B2C cross-border services)
       ↓
Annual Reconciliation
```

### Excise Movement Chain
```
AWK Application (Warehouse Authorization)
       ↓
EMCS Registration
       ↓
e-AD Submission (intra-EU duty-suspended)
       ↓
Receipt Confirmation (by destination)
       ↓
OR: IAAD (domestic duty-suspended movement)
```

---

## 6. Digital vs Paper Status Matrix

| Form Category | Form Codes | Electronic Status | Paper Available | Notes |
|---------------|------------|-------------------|-----------------|-------|
| **Fully Electronic (Mandatory)** | | | | |
| Company Tax Returns | e-CO, TIFD | **Mandatory** since 2002 | No | Via CFR portal |
| VAT Returns (Art 10) | VAT Return | **Mandatory** since Feb 2023 | No | Via CFR e-Services |
| Recapitulative Statements | EC Sales List | **Mandatory** since 2009 | No | LN 363/2009 |
| Intrastat | Arrivals/Dispatches | **Mandatory** | No | Via NSO system |
| Transit Declarations | T1, T2, T2L | **Mandatory** | No | Via NCTS-P5 |
| Excise Movements | e-AD, e-SAD | **Mandatory** | No | Via EMCS |
| ENS/EXS | Entry/Exit Summary | **Mandatory** | No | Via ICS2 |
| OSS/IOSS | All OSS returns | **Mandatory** | No | Via CFR portal |
| BTI Applications | BTI | **Mandatory** since Oct 2019 | No | Via EBTI |
| CRS/FATCA | XML reporting | **Mandatory** (&gt;100 accounts) | No | OECD XSD v2.0 |
| DAC6/DAC7 | Reportable arrangements | **Mandatory** | No | XML schema |
| **Electronic Preferred** | | | | |
| FSS Employer Forms | FS3, FS5, FS7 | **Mandatory** (FS5 all; others 10+ employees) | Yes (&lt;10 employees) | Via MTCA portal |
| FS4 | Payee Status | Electronic preferred | Yes | Via MTCA portal |
| **Both Paper and Electronic** | | | | |
| Individual Tax Returns | e-Return | Online (myTax) | By request | Deadline differs |
| Part-Time Forms | TA22, TA23, TA24 | Online option | Yes | Both accepted |
| Tax Credit Forms | RA series | With return | Yes | Submitted with return |
| **Paper Only/Predominantly** | | | | |
| Property Transfers | DDT1, Schedule B/B1 | Paper to CTD | Paper | Via notary |
| Origin Certificates | EUR.1, Form A | Paper (stamped) | Yes | Physical stamps required |
| Excise Domestic | IAAD | PDF download | Yes | Downloadable form |
| AEO Application | AEO | Via EU CDS portal | No | EU-wide system |

---

## 7. XML Schemas and Data Exchange Formats

### Schemas Published/Used by Malta CFR

| Schema | Version | Standard | Use | Format |
|--------|---------|----------|-----|--------|
| **CRS Schema** | OECD XSD v2.0 | OECD CRS | Financial account reporting (DAC2) | XML |
| **CbCR Schema** | OECD XSD v2.0 | OECD BEPS Action 13 | Country-by-Country Reporting | XML |
| **FATCA Schema** | OECD XSD v2.0 | IRS/OECD | US account reporting | XML |
| **DAC6 Schema** | EU Standard | Council Directive 2018/822 | Cross-border arrangements | XML |
| **DAC7 Schema** | EU Standard | Council Directive 2021/514 | Platform operator reporting | XML |
| **TIFD** | Malta-specific | CFR | Company financial data | Excel/Structured |

**Note on SAF-T:** Malta has **NOT** implemented the OECD Standard Audit File for Tax. Instead, the proprietary **TIFD (Tax Index of Financial Data)** format is used for corporate tax return attachments since 2002.

### E-Invoicing Status
Malta has no mandatory B2B/B2G e-invoicing requirement, though public authorities must accept **EN 16931** compliant invoices per LN 403-404/2018. The **ViDA Directive (2025/516)** will drive future e-invoicing/real-time reporting requirements with Malta targeting VIDA readiness by 2030.

---

## 8. Specific Questions Answered

### Q1: Tax forms introduced or retired between 2018-2025?

**Introduced:**
- TRA 100/100A (NID forms) — 2018
- DAC6 XML reporting forms — 2019
- Fiscal Unit documents — 2019
- RA26 (COVID business continuity) — 2020
- OSS/IOSS returns (replacing MOSS) — July 2021
- DAC7 platform operator forms — 2023
- TRA 131-134 (new credits) — 2025
- BCRS producer registration — November 2022

**Retired/Superseded:**
- MOSS quarterly returns — Replaced by OSS (June 2021)
- TA24A (rental failure to declare) — Superseded by standard TA24 process
- Paper-only VAT returns — Electronic mandatory February 2023

### Q2: Mandatory vs optional data fields?

**Mandatory across all forms:** TIN/VAT number, accounting period, taxpayer name/address, submission date

**Context-dependent mandatory fields:**
- EORI: Required in SAD boxes 2, 8, 14 for customs declarations
- EU Customer VAT: Required in Recapitulative Statements for each supply
- Director TIN (EU): Now mandatory in TRA 82 from YA 2023
- Transfer pricing disclosure: Mandatory on Page 2 from YA 2025

### Q3: Which forms are fully digitized vs paper-only?

See Section 6 above for complete matrix. Key points:
- **100% electronic:** Transit (NCTS), Excise (EMCS), VAT Art 10 returns, Intrastat, OSS, CRS/FATCA, DAC6/7
- **Predominantly paper:** Property transfers (DDT1), origin certificates (EUR.1)
- **Transitioning:** Individual tax returns (online encouraged), TA series (both available)

### Q4: Annual filing volumes per form type?

Not publicly available in aggregate. Malta's economy profile suggests approximately:
- **~200,000** individual tax returns annually
- **~50,000** corporate tax returns (registered companies)
- **~30,000** Article 10 VAT registrations
- **~20,000** Article 11 small undertaking registrations

### Q5: Mapping to EU-standard returns?

| Malta Form | EU Standard | Legal Basis |
|------------|-------------|-------------|
| VAT Return | VAT Directive Art. 250-261 | 2006/112/EC |
| Recapitulative Statement | EC Sales List (Art. 262-263) | 2006/112/EC |
| Intrastat | Intrastat Regulation | 2019/2152 |
| OSS Returns | Union/Non-Union/Import OSS | 2017/2455, 2019/1995 |
| SAD | Union Customs Code SAD | 952/2013 |
| e-AD | Excise Movement Document | 2020/262 |
| CRS Reporting | DAC2 | 2014/107/EU |
| CbCR | DAC4 | 2016/881/EU |
| DAC6 | Reportable Arrangements | 2018/822 |
| DAC7 | Platform Operators | 2021/514 |

### Q6: XML/data schemas for electronic submissions?

Primary schemas: OECD XSD v2.0 (CRS, CbCR, FATCA), EU DAC6/DAC7 schemas, NSO Intrastat XML. Malta does **not** use SAF-T; instead uses proprietary TIFD format for corporate returns.

### Q7: Common taxpayer identifiers across forms?

| Identifier | Primary Use | Cross-Referenced In |
|------------|-------------|---------------------|
| **TIN** | All income tax forms | FSS, Property transfers, CRS |
| **VAT Number** | All VAT forms | Customs (as EORI), OSS, Intrastat |
| **EORI** | All customs forms | Derived from VAT number |
| **Company ROC** | MBR registration | Corporate returns, share transfers |

---

## 9. ORS Schema Design Recommendations

Based on this catalogue, key schema design considerations for the Operational Reporting System include:

1. **Unified taxpayer master table** with TIN as primary key, supporting both 8-character (Maltese individual) and 9-character (non-Maltese/corporate) formats, with derived VAT/EORI numbers.

2. **Tax account tracking** for corporate entities requiring five distinct accounts (FTA, MTA, FIA, IPA, UA) with historical balances for imputation refund calculations.

3. **Form versioning system** to track TRA form changes across assessment years (e.g., TRA 63 modifications in YA 2024).

4. **XML schema compatibility** for DAC6/DAC7/CRS imports using OECD XSD v2.0 standard structures.

5. **Electronic submission status flags** distinguishing mandatory electronic (no paper fallback) from optional electronic (paper available) forms for workflow routing.

6. **EU reporting calendar** automation for monthly Intrastat, quarterly VAT/OSS, and annual reconciliation deadlines with different submission windows.

7. **Cross-form validation rules** ensuring FS3/FS5/FS7 reconciliation, TRA 19/20 group relief matching, and Schedule B-F capital gains consistency.

This catalogue provides the foundational reference for MTCA's digital transformation, ensuring comprehensive coverage of Malta's tax form ecosystem for schema design and Informix data migration planning.
