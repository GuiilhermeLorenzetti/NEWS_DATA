import statsmodels.stats.api as sms
import math

def get_valid_number(prompt_text):
    """
    Repeatedly asks the user for input until a valid number is provided.
    """
    while True:
        user_input = input(prompt_text)
        try:
            # Try to convert string input to a float
            value = float(user_input)
            return value
        except ValueError:
            # If conversion fails, print error and loop again
            print("❌ Invalid input. Please enter a number (e.g., 10 or 5.5).")

def calculate_sample_size():
    print("--- A/B Test Sample Size Calculator ---")
    print("Please enter values as percentages (e.g., write 10 for 10%).\n")

    # 1. Get Baseline Conversion Rate
    baseline_input = get_valid_number("1. What is your current Baseline Conversion Rate? (%) ")
    baseline_rate = baseline_input / 100.0

    # 2. Get Minimum Detectable Effect (MDE)
    # Note: This is relative lift. If you want to go from 10% to 12%, that is a 20% lift.
    mde_input = get_valid_number("2. What is the Minimum Detectable Effect (Lift) you want to detect? (%) ")
    mde = mde_input / 100.0

    # 3. Get Confidence Level (Alpha) - usually 95%
    confidence_input = get_valid_number("3. What Confidence Level do you want? (Standard is 95%) ")
    alpha = 1 - (confidence_input / 100.0)

    # 4. Get Statistical Power - usually 80%
    power_input = get_valid_number("4. What Statistical Power do you want? (Standard is 80%) ")
    power = power_input / 100.0

    # --- Calculation ---
    target_rate = baseline_rate * (1 + mde)
    
    # Calculate Effect Size
    effect_size = sms.proportion_effectsize(baseline_rate, target_rate)
    
    # Calculate Sample Size
    sample_size = sms.NormalIndPower().solve_power(
        effect_size=effect_size, 
        power=power, 
        alpha=alpha, 
        ratio=1
    )
    
    required_n = math.ceil(sample_size)

    # --- Display Results ---
    print("\n" + "="*40)
    print("       RESULTS       ")
    print("="*40)
    print(f"Current Rate:    {baseline_rate:.2%}")
    print(f"Target Rate:     {target_rate:.2%}")
    print("-" * 40)
    print(f"Sample Size Needed: {required_n:,} per variant")
    print(f"Total Traffic:      {required_n * 2:,} total visitors")
    print("="*40)

if __name__ == "__main__":
    calculate_sample_size()