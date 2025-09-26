"""
label_with_context.py

Purpose
-------
Classify interview "Q / R" pairs into a **fixed taxonomy** of labels using GPT,
with light context from the previous Q and next R to improve accuracy.

Why this version?
-----------------
- Engineering-friendly: no hardcoded secrets; uses OPENAI_API_KEY from env
- Robust I/O: works on alternating header/content rows produced by your pipeline
- Strict outputs: model must return one canonical label or "Unmatched"
- Retry logic for transient API errors/rate limits
- Clean logging + CLI arguments for reproducibility

Usage (defaults to your current path):
    python label_with_context.py \
        --input "/Users/buzhaoliu/Developer/NLP/interview_data_raw.xlsx" \
        --output "/Users/buzhaoliu/Developer/NLP/interview_data_labeled.xlsx" \
        --model "gpt-4o-mini"

Notes
-----
- Keep real interview data private. Commit only code + synthetic samples to GitHub.
- If you change the label set, update LABEL_CODES and LABEL_PROMPT_ENUM below.
"""

import os
import re
import time
import argparse
import json
from typing import Dict, List, Tuple, Optional

import pandas as pd

# ======= Canonical label set (codes) =======
LABEL_CODES: List[str] = [
    "Intro",
    "Current_Status",
    "Wife_Or_You",
    "End_Year",
    "Daily_Tasks",
    "Ward_Languages",
    "Mother_Tongue",
    "Other_Lang_Spoken",
    "Other_Lang_Understood",
    "Hindi_Dialects",
    "Hindi_Dialect_Differences",
    "Correct_Hindi",
    "Dialect_Job_Impact",
    "Dialect_Constituents_1on1_Impact",
    "Dialect_Constituents_Groups_Impact",
    "Dialect_Corp_Impact",
    "Dialect_Leaders_Impact",
    "Dialect_Choice_Example",
    "Dialect_Choice_Grievance",
    "Dialect_Choice_Campaigning",
    "Dialect_Choice_Meetings",
    "Dialect_Choice_Bureaucrats",
    "Dialect_Choice_Other",
    "Dialect_Choice_Multiple",
    "Unmatched",
    "Thanks",
]
LABEL_SET = set(LABEL_CODES)

# For the prompt, we enumerate allowed labels only (not full questions)
LABEL_PROMPT_ENUM = "\n".join(f"- {c}" for c in LABEL_CODES)

# ======= OpenAI client (env var only; never hardcode keys) =======
# Supports both old openai==0.x and new openai>=1.0 clients gracefully
def _get_openai_client():
    try:
        # New SDK style
        from openai import OpenAI  # type: ignore
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY env var not set.")
        return OpenAI(api_key=api_key), "new"
    except Exception:
        # Fallback to legacy interface
        import openai  # type: ignore
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY env var not set.")
        openai.api_key = api_key
        return openai, "legacy"

CLIENT, CLIENT_MODE = _get_openai_client()

# ======= GPT call with retries & strict parsing =======
def classify_with_gpt(
    question: str,
    prev_question: str,
    next_response: str,
    model: str = "gpt-4o-mini",
    max_retries: int = 5,
    backoff_sec: float = 2.0,
) -> str:
    """
    Ask GPT to choose exactly ONE label code from LABEL_CODES (or 'Unmatched').
    Returns a label string from LABEL_CODES.
    """
    system = (
        "You label interview questions into ONE label from a fixed set. "
        "Return ONLY the label code, nothing else."
    )

    # Engineering prompt: short, deterministic, constrained
    user = f"""
Task: Choose ONE best label code for the CURRENT QUESTION from the allowed list.

Allowed label codes:
{LABEL_PROMPT_ENUM}

Context for disambiguation (optional to use):
- PREVIOUS QUESTION: {prev_question.strip() if prev_question else "None"}
- CURRENT QUESTION: {question.strip() if question else ""}
- NEXT RESPONSE: {next_response.strip() if next_response else "None"}

Output format:
Return ONLY one of the allowed label codes above (exact case), or "Unmatched".
"""

    for attempt in range(1, max_retries + 1):
        try:
            if CLIENT_MODE == "new":
                # New SDK
                resp = CLIENT.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    temperature=0,
                    max_tokens=8,
                )
                text = resp.choices[0].message.content.strip()
            else:
                # Legacy SDK
                text = CLIENT.ChatCompletion.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    temperature=0,
                    max_tokens=8,
                )["choices"][0]["message"]["content"].strip()

            # Normalize & validate
            text = text.replace("\u00A0", " ").strip()
            # Keep only first token-ish if model returns extra words
            text = re.split(r"[\s\n\r]+", text)[0]
            return text if text in LABEL_SET else "Unmatched"

        except Exception as e:
            if attempt == max_retries:
                print(f"[ERROR] GPT call failed after {attempt} tries: {e}")
                return "Unmatched"
            sleep_for = backoff_sec * (2 ** (attempt - 1))
            time.sleep(sleep_for)

# ======= Spreadsheet parsing helpers =======
HEADER_RE = re.compile(r"^\s*([RQ])\s*_(\d+)\s*_(.+?)\s*$")

def parse_header(cell_val: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Parse headers like 'Q_12_Label' or 'R_7_Label' -> (kind, idx, label).
    Returns (None,None,None) if not a header.
    """
    if cell_val is None:
        return None, None, None
    text = str(cell_val).strip().replace("\u00A0", " ")
    m = HEADER_RE.match(text)
    if not m:
        return None, None, None
    kind, idx, label = m.group(1), m.group(2), m.group(3)
    return kind, idx, label

# ======= Main processing =======
def process_file(
    input_path: str,
    output_path: str,
    model: str = "gpt-4o-mini",
) -> None:
    """
    Reads an interview Excel file with alternating rows:
      row i   -> headers (Name, Date, Location, Q_/R_ headers...)
      row i+1 -> content  (Name, Date, Location, text...)

    Produces a labeled version where headers become Q_k_<LABEL> / R_k_<LABEL>
    based on GPT classification of each Q cell (context: prev Q + next R).
    """
    df = pd.read_excel(input_path, header=None)
    out_rows: List[List[str]] = []

    for i in range(0, len(df), 2):
        if i + 1 >= len(df):
            break

        headers = df.iloc[i].tolist()
        row_vals = df.iloc[i + 1].tolist()

        # Preserve first three metadata columns
        meta_headers = headers[:3]
        meta_values = row_vals[:3]

        # Build new header row
        new_headers: List[str] = list(meta_headers)
        new_content: List[str] = list(meta_values)

        # Walk across Q/R columns starting at index 3
        # We label based on the *question cell text* in the content row.
        # For context, we use the previous Q text and next R text when available.
        q_counter = 1
        col = 3
        while col < len(headers):
            h = headers[col]
            kind, idx, label_in_header = parse_header(h)

            # Expect a question at this position; if not, advance
            if kind != "Q":
                col += 1
                continue

            # Grab current question text (same column in content row)
            question_text = str(row_vals[col]).strip() if col < len(row_vals) and pd.notna(row_vals[col]) else ""

            # Previous question text for context (look back to previous Q column)
            prev_q_text = ""
            back = col - 1
            while back >= 3:
                kk, _, _ = parse_header(headers[back])
                if kk == "Q":
                    prev_q_text = str(row_vals[back]).strip() if pd.notna(row_vals[back]) else ""
                    break
                back -= 1

            # Next response text for context (look forward to next R column)
            next_r_text = ""
            fwd = col + 1
            while fwd < len(headers):
                kk, _, _ = parse_header(headers[fwd])
                if kk == "R":
                    next_r_text = str(row_vals[fwd]).strip() if fwd < len(row_vals) and pd.notna(row_vals[fwd]) else ""
                    break
                fwd += 1

            # Classify the CURRENT QUESTION
            label_code = classify_with_gpt(
                question=question_text,
                prev_question=prev_q_text,
                next_response=next_r_text,
                model=model,
            )

            # Create standardized headers using our index counter (natural order)
            q_col_name = f"Q_{q_counter}_{label_code}"
            r_col_name = f"R_{q_counter}_{label_code}"
            new_headers.extend([q_col_name, r_col_name])

            # Append the actual question/response values (if present)
            q_val = row_vals[col] if col < len(row_vals) else ""
            r_val = row_vals[col + 1] if (col + 1) < len(row_vals) else ""
            new_content.extend([q_val, r_val])

            q_counter += 1

            # Advance at least two columns if this was a clean Q/R pair
            col += 2

        # Store the transformed two-row block
        out_rows.append(new_headers)
        out_rows.append(new_content)

    pd.DataFrame(out_rows).to_excel(output_path, index=False, header=False)
    print(f"✅ Labeled file written to: {output_path}")

# ======= CLI =======
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",
                        default="/Users/buzhaoliu/Developer/NLP/interview_data_raw.xlsx",
                        help="Path to the input Excel file (alternating header/content rows).")
    parser.add_argument("--output",
                        default="/Users/buzhaoliu/Developer/NLP/interview_data_labeled.xlsx",
                        help="Path to write the labeled Excel file.")
    parser.add_argument("--model",
                        default="gpt-4o-mini",
                        help="OpenAI chat model (e.g., gpt-4o, gpt-4o-mini).")
    args = parser.parse_args()
    process_file(args.input, args.output, model=args.model)

if __name__ == "__main__":
    main()
