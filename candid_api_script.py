import pandas as pd
import requests
import time
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("CANDID_API_KEY")
API_URL = os.getenv("CANDID_API_URL", "https://api.candid.org/premier/v3")
INPUT_CSV = os.getenv("INPUT_CSV", "ein_list.csv")
OUTPUT_CSV = os.getenv("OUTPUT_CSV", "ein_output.csv")

if not API_KEY:
	raise ValueError("CANDID_API_KEY not found in .env file")

df = pd.read_csv(INPUT_CSV)

# script looks for a column called EIN - either change your source file column or adapt the code here
if "EIN" not in df.columns:
	raise ValueError("CSV file must contain an 'EIN' column")

results = []

for ein in df["EIN"].astype(str):
	url = f"{API_URL}/{ein}"
	headers = {"accept": "application/json", "Subscription-Key": API_KEY}
	
	try:
		response = requests.get(url, headers=headers)
		response.raise_for_status()
		data = response.json()
		
		if "data" in data:
			summary = data["data"].get("summary", {})
			most_recent_year_financials = data["data"].get("financials", {}).get("most_recent_year_financials", {})
			f990_financials = data["data"].get("financials", {}).get("f990_financials", [{}])[0]
			part_9_expenses = f990_financials.get("part_9_expenses", {})
			part_10_balance_sheet = f990_financials.get("part_10_balance_sheet", {})
			pf990_financials = data["data"].get("financials", {}).get("pf990_financials", {}) or {}
			liquidity = data["data"].get("financials", {}).get("financial_trends_analysis", [{}])[0].get("capital_structure_indicators", {}).get("liquidity", {})
			balance_sheet_composition = data["data"].get("financials", {}).get("financial_trends_analysis", [{}])[0].get("capital_structure_indicators", {}).get("balance_sheet_composition", {})
			
			results.append({
				"EIN": ein,
				"Organization Name": summary.get("organization_name", ""),
				"City": summary.get("city", ""),
				"State": summary.get("state", ""),
				"Unrestricted Net Assets (Most Recent Year)": most_recent_year_financials.get("unrestricted_net_assets", ""),
				"Months of Cash (Most Recent Year)": most_recent_year_financials.get("months_of_cash", ""),
				"Expenses Total (Most Recent Year)": most_recent_year_financials.get("expenses_total", ""),
				"Cash and Equivalent Assets": most_recent_year_financials.get("cash_and_equivalent_assets", ""),
				"Investments US Government": most_recent_year_financials.get("investments_us_government", ""),
				"Investments Stock": most_recent_year_financials.get("investments_stock", ""),
				"Investments Bonds": most_recent_year_financials.get("investments_bonds", ""),
				"Investments Other": most_recent_year_financials.get("investments_other", ""),
				"Land Buildings Equipment": most_recent_year_financials.get("land_buildings_equipment", ""),
				"Other Assets": most_recent_year_financials.get("other_assets", ""),
				"Expenses Total (F990)": f990_financials.get("expenses_total", ""),
				"Unrestricted Net Assets (F990)": f990_financials.get("unrestricted_net_assets", ""),
				"Net Fixed Assets LBE": f990_financials.get("net_fixed_assets_LBE", ""),
				"Notes Payable Mortgages": f990_financials.get("notes_payable_mortgages", ""),
				"Months of Cash (F990)": f990_financials.get("months_of_cash", ""),
				"Expenses Total (Part 9)": part_9_expenses.get("expenses_total", ""),
				"Depreciation Total": part_9_expenses.get("depreciation_total", ""),
				"Cash EOY": part_10_balance_sheet.get("cash_eoy", ""),
				"Savings EOY": part_10_balance_sheet.get("savings_eoy", ""),
				"Less Depreciation": part_10_balance_sheet.get("less_depreciation", ""),
				"LBE EOY": part_10_balance_sheet.get("lbe_eoy", ""),
				"Tax Exempt Bonds EOY": part_10_balance_sheet.get("tax_exempt_bonds_eoy", ""),
				"Secured Notes Payable EOY": part_10_balance_sheet.get("secured_notes_payable_eoy", ""),
				"Unrestricted EOY": part_10_balance_sheet.get("unrestricted_eoy", ""),
				"Cash Equivalent Investible Assets (PF990)": pf990_financials.get("cash_equivalent_investible_assets", ""),
				"Total Expenses (PF990)": pf990_financials.get("total_expenses", ""),
				"Months of Cash (Liquidity)": liquidity.get("months_of_cash", ""),
				"Months of Cash and Investments": liquidity.get("months_of_cash_and_investments", ""),
				"Months of Estimated Liquid Unrestricted Net Assets": liquidity.get("months_of_estimated_liquid_unrestricted_net_assets", ""),
				"Cash (Balance Sheet)": balance_sheet_composition.get("cash", ""),
				"Investments (Balance Sheet)": balance_sheet_composition.get("investments", ""),
				"Receivables (Balance Sheet)": balance_sheet_composition.get("receivables", ""),
				"Gross Land Buildings and Equipment LBE": balance_sheet_composition.get("gross_land_buildings_and_equipment_lbe", ""),
				"Unrestricted Net Assets (Balance Sheet)": balance_sheet_composition.get("unrestricted_net_assets", "")
			})
		else:
			print(f"No data found for EIN {ein}")
	except requests.exceptions.RequestException as e:
		print(f"Error fetching data for EIN {ein}: {e}")
	
	time.sleep(1)

output_df = pd.DataFrame(results)
output_df.to_csv(OUTPUT_CSV, index=False)

print(f"Data extraction complete. Results saved to {OUTPUT_CSV}.")