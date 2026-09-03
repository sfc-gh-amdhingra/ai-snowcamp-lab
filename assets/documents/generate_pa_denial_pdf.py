#!/usr/bin/env python3
"""
Generates assets/documents/pa_denial_letter.pdf — the synthetic prior
authorization denial letter used in Step 5d of the lab (AI_EXTRACT demo).

The lab does NOT run this script. The generated PDF is committed to the repo
and staged from the Git repository stage during Step 5a. Run this only if you
want to change the document.

    pip install fpdf2
    python3 assets/documents/generate_pa_denial_pdf.py

All member, prescriber, and clinical values are fabricated. No real PHI.
"""

from fpdf import FPDF

OUT_PATH = 'assets/documents/pa_denial_letter.pdf'

# Watermark appearance. Verified that the watermark does not affect AI_EXTRACT
# accuracy — it is a separate text object in the PDF layer, not an image
# overlaying the content, so extraction reads it as an unlabelled stray string
# and ignores it. Re-test extraction if you raise this much further.
WATERMARK_TEXT = 'SAMPLE - NOT REAL PHI'
WATERMARK_OPACITY = 0.13
WATERMARK_GRAY = (95, 95, 95)
WATERMARK_SIZE = 58

NAVY = (0, 51, 102)
ORANGE = (255, 127, 0)
RED = (200, 30, 30)
TABLE_HEADER_FILL = (230, 236, 244)


class PADenialPDF(FPDF):
    def header(self):
        """Called automatically on every page, including auto page breaks.
        Draws only the watermark; the navy branding block is applied to
        page 1 explicitly via header_block()."""
        x, y = self.get_x(), self.get_y()
        with self.local_context(fill_opacity=WATERMARK_OPACITY,
                                stroke_opacity=WATERMARK_OPACITY):
            self.set_font('Helvetica', 'B', WATERMARK_SIZE)
            self.set_text_color(*WATERMARK_GRAY)
            with self.rotation(45, self.w / 2, self.h / 2):
                self.set_xy(0, self.h / 2 - 18)
                self.cell(self.w, 20, WATERMARK_TEXT, align='C')
        self.set_text_color(0, 0, 0)
        self.set_xy(x, y)

    def header_block(self):
        self.set_fill_color(*NAVY)
        self.rect(0, 0, 210, 28, 'F')
        self.set_text_color(255, 255, 255)
        self.set_font('Helvetica', 'B', 18)
        self.set_y(5)
        self.cell(0, 10, 'UnitedHealthcare', new_x='LMARGIN', new_y='NEXT')
        self.set_font('Helvetica', '', 10)
        self.cell(0, 8, 'Commercial Plans | Prior Authorization Program',
                  new_x='LMARGIN', new_y='NEXT')
        self.set_fill_color(*ORANGE)
        self.rect(0, 28, 210, 2, 'F')
        self.set_y(35)
        self.set_text_color(0, 0, 0)

    def section_header(self, title):
        self.set_fill_color(*NAVY)
        self.set_text_color(255, 255, 255)
        self.set_font('Helvetica', 'B', 11)
        self.cell(0, 8, '  ' + title, fill=True, new_x='LMARGIN', new_y='NEXT')
        self.set_text_color(0, 0, 0)
        self.ln(2)

    def info_table(self, rows, cw=(55, 135)):
        for label, value in rows:
            self.set_font('Helvetica', 'B', 10)
            self.cell(cw[0], 6.5, label, border='B')
            self.set_font('Helvetica', '', 10)
            self.cell(cw[1], 6.5, value, border='B',
                      new_x='LMARGIN', new_y='NEXT')
        self.ln(3)

    def data_table(self, headers, rows, cw=None):
        if cw is None:
            cw = [190 / len(headers)] * len(headers)
        self.set_fill_color(*TABLE_HEADER_FILL)
        self.set_font('Helvetica', 'B', 9)
        for i, h in enumerate(headers):
            self.cell(cw[i], 7, h, border=1, fill=True, align='C')
        self.ln()
        self.set_font('Helvetica', '', 9)
        for row in rows:
            for i, val in enumerate(row):
                self.cell(cw[i], 6.5, str(val), border=1, align='C')
            self.ln()
        self.ln(3)


def build():
    pdf = PADenialPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.header_block()

    pdf.set_font('Helvetica', 'B', 14)
    pdf.cell(0, 10, 'Prior Authorization Decision Notice',
             new_x='LMARGIN', new_y='NEXT', align='C')
    pdf.set_font('Helvetica', '', 10)
    pdf.cell(95, 7, 'Date: March 22, 2025')
    pdf.cell(95, 7, 'PA Reference: PA-2025-0847291', align='R',
             new_x='LMARGIN', new_y='NEXT')
    pdf.ln(2)

    pdf.set_fill_color(*RED)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font('Helvetica', 'B', 16)
    pdf.cell(60, 12, '  DECISION: DENIED', fill=True,
             new_x='LMARGIN', new_y='NEXT')
    pdf.set_text_color(0, 0, 0)
    pdf.ln(4)

    pdf.section_header('MEMBER INFORMATION')
    pdf.info_table([
        ('Member Name:', 'Sarah Mitchell'),
        ('Member ID:', 'MBR004523'),
        ('Date of Birth:', '06/14/1978'),
        ('Plan:', 'UHC Choice Plus PPO (PLN-PPO-003)'),
        ('PCP:', 'Dr. James Rivera, MD - Internal Medicine'),
    ])

    pdf.section_header('PRESCRIBER INFORMATION')
    pdf.info_table([
        ('Provider:', 'Dr. Linda Chen, MD'),
        ('Specialty:', 'Endocrinology'),
        ('NPI:', '1234567890'),
        ('Provider ID:', 'PRV000847'),
        ('Facility:', 'Optum Care Endocrinology - Dallas, TX'),
    ])

    pdf.section_header('MEDICATION REQUESTED')
    pdf.data_table(
        ['Drug Name', 'NDC', 'Drug Class', 'Qty / Supply', 'Est. Cost'],
        [['Ozempic (semaglutide) 1mg', '00169-4132-12',
          'GLP-1 / Diabetes', '3mL / 28 days', '$892.47']],
        cw=[52, 32, 36, 34, 26]
    )

    pdf.section_header('CLINICAL VALUES')
    pdf.data_table(
        ['Measure', 'Value', 'Date', 'Reference Range'],
        [
            ['HbA1c', '8.4%', '02/28/2025', '< 7.0% (ADA target)'],
            ['BMI', '34.2', '02/28/2025', '18.5 - 24.9'],
            ['Fasting Glucose', '186 mg/dL', '02/28/2025', '70 - 100 mg/dL'],
            ['eGFR', '82 mL/min', '02/28/2025', '> 60 mL/min'],
        ],
        cw=[42, 32, 36, 70]
    )

    pdf.set_font('Helvetica', 'B', 10)
    pdf.cell(0, 7, 'Primary Dx: Type 2 Diabetes Mellitus (ICD-10: E11.9)',
             new_x='LMARGIN', new_y='NEXT')
    pdf.cell(0, 7, 'Secondary Dx: Obesity (ICD-10: E66.01)',
             new_x='LMARGIN', new_y='NEXT')
    pdf.ln(2)

    pdf.section_header('STEP THERAPY STATUS')
    pdf.set_font('Helvetica', '', 9)
    pdf.multi_cell(0, 5.5, 'Per UHC Formulary Policy FRM-2025-GLP1, '
                           'Section 4.2, the following step therapy must be '
                           'completed:')
    pdf.ln(2)
    pdf.data_table(
        ['Step', 'Medication', 'Tier', 'Required Duration', 'Patient Status'],
        [
            ['Step 1', 'Metformin', 'Tier 1 (Generic)',
             '90 days at max dose', 'COMPLETED (26 mo)'],
            ['Step 2', 'Sulfonylurea (Glipizide)', 'Tier 2 (Preferred)',
             '90 days at max dose', 'INCOMPLETE *'],
            ['Step 3', 'GLP-1 (Ozempic)', 'Tier 5 (Specialty)',
             'PA Required', 'REQUESTED'],
        ],
        cw=[18, 46, 36, 40, 50]
    )
    pdf.set_font('Helvetica', 'I', 9)
    pdf.multi_cell(0, 5.5, '* Glipizide prescribed at 10mg daily for 9 months. '
                           'Max dose is 20mg daily. Records do not document '
                           'titration to max dose or clinical failure at '
                           'current dose.')
    pdf.ln(3)

    pdf.section_header('DENIAL REASON')
    pdf.set_font('Helvetica', '', 10)
    pdf.multi_cell(0, 6, 'Step therapy requirement not met. GLP-1 receptor '
                         'agonists require documented failure of or '
                         'intolerance to at least TWO oral antidiabetic '
                         'agents at maximum tolerated doses for a minimum of '
                         '90 days each. Glipizide has not been titrated to '
                         'maximum dose (20mg daily) and no documentation of '
                         'clinical failure or adverse reaction at current '
                         'dose was submitted.')
    pdf.ln(2)
    pdf.set_font('Helvetica', 'I', 9)
    pdf.multi_cell(0, 5.5, 'Clinical Review Basis: NCQA/HEDIS Comprehensive '
                           'Diabetes Care guidelines, UHC Clinical Policy '
                           'Bulletin CPB-0372 (GLP-1 Receptor Agonists), UHC '
                           'Commercial Formulary eff. 01/01/2025.')
    pdf.ln(3)

    pdf.section_header('APPEAL RIGHTS')
    pdf.set_font('Helvetica', '', 10)
    pdf.multi_cell(0, 6, 'You have the right to appeal this decision within '
                         '180 days. Submit clinical documentation '
                         'demonstrating: (1) titration of Glipizide to '
                         'maximum tolerated dose with inadequate glycemic '
                         'control, OR (2) documented adverse reaction or '
                         'contraindication to dose escalation.')
    pdf.ln(2)
    pdf.data_table(
        ['Appeal Deadline', 'Fax', 'Portal'],
        [['September 18, 2025', '1-800-555-0147', 'uhcprovider.com/appeals']],
        cw=[65, 60, 65]
    )

    pdf.ln(2)
    pdf.set_draw_color(*NAVY)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(3)
    pdf.set_font('Helvetica', '', 8)
    pdf.cell(0, 5, 'Reviewed by: Dr. Robert Park, MD, MBA | Medical Director, '
                   'Pharmacy Services | March 22, 2025',
             new_x='LMARGIN', new_y='NEXT')
    pdf.set_font('Helvetica', 'I', 8)
    pdf.cell(0, 5, 'This document contains Protected Health Information (PHI) '
                   'subject to HIPAA regulations.',
             new_x='LMARGIN', new_y='NEXT')
    pdf.cell(0, 5, 'CONFIDENTIAL: Unauthorized disclosure prohibited under '
                   '45 CFR Parts 160 and 164.',
             new_x='LMARGIN', new_y='NEXT')

    pdf.output(OUT_PATH)
    print(f'Wrote {OUT_PATH} (watermark opacity {WATERMARK_OPACITY})')


if __name__ == '__main__':
    build()
