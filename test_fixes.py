"""
Quick test to verify all fixes work before running dashboard
"""
import logging
from engine import ResidualLoadEngine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("\n" + "="*80)
print("TESTING ALL FIXES")
print("="*80)

engine = ResidualLoadEngine()

print("\n[1] Testing ECEPS model update...")
try:
    scenarios = engine.update(model="eceps")
    
    if scenarios:
        for key, data in scenarios.items():
            if hasattr(data, 'empty'):
                status = "✓" if not data.empty else "✗"
                print(f"  {status} {key}: {data.shape if not data.empty else 'EMPTY'}")
            else:
                print(f"  → {key}: {type(data).__name__}")
        
        # Check percentiles in residual scenarios
        residual = scenarios.get('residual_scenarios', {})
        if hasattr(residual, 'columns'):
            percentile_cols = [c for c in residual.columns if c.startswith('ens_P')]
            print(f"\n  Percentile columns: {len(percentile_cols)} total")
            print(f"  {sorted(percentile_cols)[:5]}... to ..{sorted(percentile_cols)[-5:]}")
    else:
        print("  ✗ No scenarios returned")
        
except Exception as e:
    print(f"  ✗ ERROR: {e}")
    import traceback
    traceback.print_exc()

print("\n[2] Testing renewable data availability...")
try:
    renewable = engine.scenarios.get('renewables_ens', {})
    if hasattr(renewable, 'empty'):
        if not renewable.empty:
            print(f"  ✓ {renewable.shape[0]} hours of renewable data")
            total_ren_cols = [c for c in renewable.columns if 'total_ren_ens_' in c]
            print(f"  ✓ {len(total_ren_cols)} renewable ensemble members")
        else:
            print(f"  ✗ No renewable data")
    else:
        print(f"  ✗ Renewable data is {type(renewable)}")
except Exception as e:
    print(f"  ✗ ERROR: {e}")

print("\n[3] Testing consumption data...")
try:
    consumption = engine.scenarios.get('consumption', {})
    if hasattr(consumption, 'empty'):
        if not consumption.empty:
            print(f"  ✓ {consumption.shape[0]} hours of consumption data")
        else:
            print(f"  ✗ No consumption data")
    else:
        print(f"  ✗ Consumption data is {type(consumption)}")
except Exception as e:
    print(f"  ✗ ERROR: {e}")

print("\n" + "="*80)
print("Ready to reload dashboard!")
print("="*80 + "\n")
