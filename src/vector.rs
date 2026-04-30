use crate::json::Payload;

#[inline(always)]
fn round4(x: f32) -> f32 {
    (x * 10000.0).round() * 0.0001
}

#[inline(always)]
fn clamp01(v: f32) -> f32 {
    round4(v.clamp(0.0, 1.0))
}

#[inline(always)]
fn mcc_risk_f(mcc: u32) -> f32 {
    match mcc {
        5411 => 0.15,
        5812 => 0.30,
        5912 => 0.20,
        5944 => 0.45,
        7801 => 0.80,
        7802 => 0.75,
        7995 => 0.85,
        4511 => 0.35,
        5311 => 0.25,
        5999 => 0.50,
        _ => 0.50,
    }
}

pub fn vectorize(p: &Payload) -> [f32; 14] {
    let mut v = [0.0f32; 14];

    v[0] = clamp01(p.amount / 10_000.0);
    v[1] = clamp01(p.installments as f32 / 12.0);
    let ratio = if p.customer_avg_amount > 0.0 {
        (p.amount / p.customer_avg_amount) / 10.0
    } else {
        1.0
    };
    v[2] = clamp01(ratio);
    v[3] = round4(p.hour as f32 / 23.0);
    v[4] = round4(p.day_of_week as f32 / 6.0);
    if p.has_last_tx {
        v[5] = clamp01(p.minutes_since_last as f32 / 1440.0);
        v[6] = clamp01(p.km_from_current / 1000.0);
    } else {
        v[5] = -1.0;
        v[6] = -1.0;
    }
    v[7] = clamp01(p.km_from_home / 1000.0);
    v[8] = clamp01(p.tx_count_24h as f32 / 20.0);
    v[9] = if p.is_online { 1.0 } else { 0.0 };
    v[10] = if p.card_present { 1.0 } else { 0.0 };
    v[11] = if p.is_unknown_merchant { 1.0 } else { 0.0 };
    v[12] = mcc_risk_f(p.mcc);
    v[13] = clamp01(p.merchant_avg_amount / 10_000.0);

    v
}
