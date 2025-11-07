# TA-RDM Test Data Generator - Quick Start

## 1. Install Dependencies

```bash
cd test_data_generator
pip install -r requirements.txt
```

## 2. Configure Database

```bash
cp .env.example .env
# Edit .env with your database password
```

## 3. Generate Test Data

```bash
# Generate everything (84 reference records + 5 parties)
python main.py --phase foundation
```

**That's it!** ✅

---

## Generated Data Summary

### Phase A.1: Reference Data (84 records)
- ✅ 1 country (Sri Lanka)
- ✅ 9 regions (provinces)
- ✅ 10 districts
- ✅ 19 localities (major cities)
- ✅ 1 currency (LKR)
- ✅ 4 genders
- ✅ 5 marital statuses
- ✅ 25 industries (ISIC codes)
- ✅ 5 legal forms
- ✅ 5 payment methods

### Phase A.2: Party Data (5 parties)
- ✅ 3 individuals (Kasun Jayasuriya, Kavindi Liyanage, Kasun Perera)
- ✅ 2 enterprises (Ceylon Hardware Mills, Gampola Textile Company)
- ✅ 5 risk profiles (LOW to HIGH ratings)
- ✅ TINs: 100000001-100000005
- ✅ Realistic NICs in old format (e.g., 933174657V)

**Total: 89 records** across party, reference, and compliance_control schemas

---

## Validation Queries

```sql
-- Quick counts
SELECT
    'Countries' AS type, COUNT(*) AS count FROM reference.ref_country
UNION ALL
SELECT 'Regions', COUNT(*) FROM reference.ref_region
UNION ALL
SELECT 'Districts', COUNT(*) FROM reference.ref_district
UNION ALL
SELECT 'Parties', COUNT(*) FROM party.party
UNION ALL
SELECT 'Individuals', COUNT(*) FROM party.individual
UNION ALL
SELECT 'Risk Profiles', COUNT(*) FROM compliance_control.taxpayer_risk_profile;

-- View all generated parties
SELECT
    p.party_id,
    p.party_name,
    p.party_type_code,
    i.tax_identification_number AS tin,
    i.gender_code,
    i.birth_date,
    r.risk_rating_code,
    r.overall_risk_score
FROM party.party p
LEFT JOIN party.individual i ON p.party_id = i.party_id
LEFT JOIN compliance_control.taxpayer_risk_profile r ON p.party_id = r.party_id
ORDER BY p.party_id;
```

---

## Re-running the Generator

If you need to regenerate data:

```bash
# 1. Clean existing data
python cleanup.py --force

# 2. Re-generate
python main.py --phase foundation
```

---

## Common Issues

### "Duplicate entry" error
**Solution:** Run cleanup first:
```bash
python cleanup.py --force
python main.py --phase foundation
```

### "INSERT command denied" error
**Solution:** Grant permissions:
```sql
GRANT ALL PRIVILEGES ON reference.* TO 'ta_testdata'@'localhost';
GRANT ALL PRIVILEGES ON party.* TO 'ta_testdata'@'localhost';
GRANT ALL PRIVILEGES ON compliance_control.* TO 'ta_testdata'@'localhost';
FLUSH PRIVILEGES;
```

### "Connection refused"
**Solution:** Check MySQL is running on port 3308:
```bash
mysql -h localhost -P 3308 -u ta_testdata -p
```

---

## Next Steps (Phase B - Future)

Phase B will add:
- Tax framework data (tax types, periods, rates)
- Registration data (tax accounts)
- Filing data (tax returns)
- Accounting transactions
- Payment/refund data

**Not yet implemented** - Phase A validates the foundation only.

---

## Support

For issues, check:
1. Database permissions are granted
2. MySQL is running on port 3308
3. .env file has correct password
4. Run `python cleanup.py --verify-only` to check state

Full documentation: See [README.md](README.md)
