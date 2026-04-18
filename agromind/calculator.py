from __future__ import annotations


class EconomicsCalculator:
    @staticmethod
    def calculate_cycle_economics(
        area_sqm: float,
        energy_price_kwh: float,
        market_price_per_kg: float,
        culture_data: dict,
    ) -> dict:
        energy_cost = area_sqm * culture_data["power_kw_per_sqm"] * energy_price_kwh
        materials_cost = area_sqm * (
            culture_data["seed_cost_per_sqm"] + culture_data["nutrition_cost_per_sqm"]
        )
        total_expenses = energy_cost + materials_cost
        expected_yield_kg = area_sqm * culture_data["yield_kg_per_sqm"]
        expected_revenue = expected_yield_kg * market_price_per_kg
        net_profit = expected_revenue - total_expenses

        return {
            "energy_cost": round(energy_cost, 2),
            "materials_cost": round(materials_cost, 2),
            "total_expenses": round(total_expenses, 2),
            "expected_yield_kg": round(expected_yield_kg, 2),
            "expected_revenue": round(expected_revenue, 2),
            "net_profit": round(net_profit, 2),
        }
