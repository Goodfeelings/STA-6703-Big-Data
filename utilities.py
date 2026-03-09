# utilities.py
import pandera.pandas as pa

# ── Schemas ──────────────────────────────────────────────────────────

base_schema = pa.DataFrameSchema({
    "PUFYEAR": pa.Column(int, checks=pa.Check.in_range(1990, 2025), nullable=False),
    "OPTIME": pa.Column(float, checks=pa.Check.gt(0), nullable=False, coerce=True),
    "AGE": pa.Column(float, checks=pa.Check.in_range(0, 120)),
    "SEX": pa.Column(str, checks=pa.Check.isin(["male", "female"]), nullable=False),
})

# comorbidity_schema = pa.DataFrameSchema({
#     **{col: pa.Column(int, checks=pa.Check.isin([0, 1]), nullable=False, coerce=True)
#        for col in [
#            'DIABETES', 'SMOKE', 'DYSPNEA', 'VENTILAT', 'HXCOPD', 'ASCITES',
#            'HXCHF', 'HYPERMED', 'RENAFAIL', 'DIALYSIS', 'DISCANCR', 'WNDINF',
#            'STEROID', 'WTLOSS', 'BLEEDIS', 'TRANSFUS'
#        ]},
#     "PRSEPIS": pa.Column(str, checks=pa.Check.isin(["None", "Sepsis", "Septic Shock", "SIRS"]), nullable=False),
#     "BMI": pa.Column(float, checks=pa.Check.in_range(10, 150), coerce=True),
# })
#
# derived_schema = pa.DataFrameSchema({
#     "SEPSIS": pa.Column(int, checks=pa.Check.isin([0, 1]), nullable=False),
#     "COMORBIDITIES": pa.Column(int, checks=pa.Check.isin([0, 1, 2, 3]), nullable=False),
# })


# ── Validation ───────────────────────────────────────────────────────

def check_data(dataframe, schema, print_rows=False):
    try:
        schema.validate(dataframe, lazy=True)
    except pa.errors.SchemaErrors as err:
        failures_df = err.failure_cases
        if print_rows:
            print("Rows with Validation Errors:")
            print(failures_df)
        summary_df = (
            failures_df.groupby(["column", "check", "failure_case"], dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        print("Summary of Validation Errors:")
        display(summary_df)
    else:
        print("No errors found.")

comorbidity_schema = pa.DataFrameSchema({
    # Comorbidity Columns are binary and should be 0 or 1.
    # The ** allows us to unpack the dictionary into the schema.
    # i.e., the same values make sense for all of the columns.
    **{col: pa.Column(
        int,
        checks=pa.Check.isin([0, 1]),
        nullable=False,
        coerce=True
    ) for col in [
        'DIABETES', 'SMOKE', 'DYSPNEA', 'VENTILAT', 'HXCOPD', 'ASCITES',
        'HXCHF', 'HYPERMED', 'RENAFAIL', 'DIALYSIS', 'DISCANCR', 'WNDINF',
        'STEROID', 'WTLOSS', 'BLEEDIS', 'TRANSFUS'
    ]},
    # PRSEPIS should be one of the expected categorical values
    "PRSEPIS": pa.Column(
        str,
        checks=pa.Check.isin(["None", "Sepsis", "Septic Shock", "SIRS"]),
        nullable=False
    ),
    # BMI should be in a reasonable range
    "BMI": pa.Column(
        float,
        checks=pa.Check.in_range(10, 150),
        coerce=True
    ),
})

derived_schema = pa.DataFrameSchema({
    "SEPSIS": pa.Column(int, checks=pa.Check.isin([0, 1]), nullable=False),
    "COMORBIDITIES": pa.Column(int, checks=pa.Check.isin([0, 1, 2, 3]),
                               nullable=False),
})