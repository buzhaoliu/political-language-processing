import pandas as pd
import re

# === Input and output paths ===
input_file = "/Users/buzhaoliu/Developer/NLP/interview_data_cleaned.xlsx"
output_file = "/Users/buzhaoliu/Developer/NLP/interview_data_merged.xlsx"

# === Load spreadsheet ===
df = pd.read_excel(input_file, header=None)

# === Extract canonical question order from Main_Questions_Labels ===
main_labels = [
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
    "Thanks"
]

# === Container for processed rows ===
all_data = []

for i in range(0, len(df), 2):  # Each interviewer block: header row + response row
    if i + 1 >= len(df):
        break
    
    headers = df.iloc[i].tolist()
    responses = df.iloc[i + 1].tolist()
    
    # Metadata: first 3 columns
    metadata = {
        "Corporator Name": responses[0] if len(responses) > 0 else "",
        "Date": responses[1] if len(responses) > 1 else "",
        "Location": responses[2] if len(responses) > 2 else ""
    }
    
    user_data = {**metadata}
    
    # Process Q/R pairs
    for h, r in zip(headers[3:], responses[3:]):
        if pd.isna(h) or pd.isna(r):
            continue
        
        # Extract label ignoring numbers (R_72_Label → Label)
        match = re.search(r"R_\d+_(.+)", str(h))
        if match:
            label = match.group(1)
        else:
            continue
        
        # Merge responses under same label
        if label not in user_data:
            user_data[label] = []
        user_data[label].append(str(r).strip())
    
    # Join responses with dash + newline
    for label in user_data:
        if isinstance(user_data[label], list):
            user_data[label] = "\n- " + "\n- ".join(user_data[label])
    
    all_data.append(user_data)

# === Convert to DataFrame ===
merged_df = pd.DataFrame(all_data)

# === Enforce column order: metadata first, then labels in main_labels order ===
final_columns = ["Corporator Name", "Date", "Location"] + main_labels
merged_df = merged_df.reindex(columns=final_columns)

# === Save to Excel ===
merged_df.to_excel(output_file, index=False)
print(f"✅ Merged responses saved to {output_file}")
