def get_roi_gross(cold_rent_year, purchase_price):
    # DE: Brutto Mietrendite
    # cold_rent_year / purchase_price
    return cold_rent_year / purchase_price * 100


def get_cashflow_after_operating_expenses(cold_rent, vacancy, house_allowance, interest, mortgage):
    # (cold_rent - vacancy_rate) - house_allowance - interest - mortgage
    return cold_rent - vacancy - house_allowance - interest - mortgage


def get_cashflow_after_taxes(cold_rent, vacancy, house_allowance, interest, mortgage, tax):
    return cold_rent - vacancy - house_allowance - interest - mortgage - tax


def get_cashflow_after_reserves(cold_rent, vacancy, house_allowance, interest, mortgage, tax, maintenance_reserve):
    return cold_rent - vacancy - house_allowance - interest - mortgage - tax - maintenance_reserve


def get_tax_year(cold_rent, vacancy, house_allowance, interest, tax_write_off, private_maintenance_reserve):
    # cold rent - vacancy - house allowance - interest - tax write off - private maintenance reserve
    return cold_rent - vacancy - house_allowance - interest - tax_write_off - private_maintenance_reserve
