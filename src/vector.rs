use crate::json::Payload;

const S: f32 = 8192.0;
const S_I16: i16 = 8192;

const INSTALLMENTS: [i16; 13] = {
    let mut lut = [0i16; 13];
    let mut i = 0;
    while i < 13 {
        lut[i] = ((i as f32 / 12.0) * S + 0.5) as i16;
        i += 1;
    }
    lut
};

const HOUR: [i16; 24] = {
    let mut lut = [0i16; 24];
    let mut i = 0;
    while i < 24 {
        lut[i] = ((i as f32 / 23.0) * S + 0.5) as i16;
        i += 1;
    }
    lut
};

const DOW: [i16; 7] = {
    let mut lut = [0i16; 7];
    let mut i = 0;
    while i < 7 {
        lut[i] = ((i as f32 / 6.0) * S + 0.5) as i16;
        i += 1;
    }
    lut
};

const TX_COUNT: [i16; 21] = {
    let mut lut = [0i16; 21];
    let mut i = 0;
    while i < 21 {
        lut[i] = ((i as f32 / 20.0) * S + 0.5) as i16;
        i += 1;
    }
    lut
};

#[inline(always)]
fn quant(v: f32) -> i16 {
    let c = if v < 0.0 {
        0.0
    } else if v > 1.0 {
        1.0
    } else {
        v
    };
    (c * S + 0.5) as i16
}

#[inline(always)]
fn mcc_risk_q(mcc: u32) -> i16 {
    match mcc {
        5411 => 1229,
        5812 => 2458,
        5912 => 1638,
        5944 => 3686,
        7801 => 6554,
        7802 => 6144,
        7995 => 6963,
        4511 => 2867,
        5311 => 2048,
        5999 => 4096,
        _ => 4096,
    }
}

#[repr(C, align(32))]
pub struct Query(pub [i16; 16]);

pub fn vectorize(p: &Payload) -> Query {
    let mut v = [0i16; 16];

    v[0] = quant(p.amount / 10_000.0);
    let inst = (p.installments as usize).min(12);
    v[1] = INSTALLMENTS[inst];
    let ratio = if p.customer_avg_amount > 0.0 {
        (p.amount / p.customer_avg_amount) / 10.0
    } else {
        1.0
    };
    v[2] = quant(ratio);
    v[3] = HOUR[(p.hour as usize).min(23)];
    v[4] = DOW[(p.day_of_week as usize).min(6)];
    if p.has_last_tx {
        v[5] = quant(p.minutes_since_last as f32 / 1440.0);
        v[6] = quant(p.km_from_current / 1000.0);
    } else {
        v[5] = -S_I16;
        v[6] = -S_I16;
    }
    v[7] = quant(p.km_from_home / 1000.0);
    let tx = (p.tx_count_24h as usize).min(20);
    v[8] = TX_COUNT[tx];
    v[9] = if p.is_online { S_I16 } else { 0 };
    v[10] = if p.card_present { S_I16 } else { 0 };
    v[11] = if p.is_unknown_merchant { S_I16 } else { 0 };
    v[12] = mcc_risk_q(p.mcc);
    v[13] = quant(p.merchant_avg_amount / 10_000.0);

    Query(v)
}
