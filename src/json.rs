use memchr::memmem;

pub struct Payload {
    pub amount: f32,
    pub installments: u8,
    pub hour: u8,
    pub day_of_week: u8,
    pub customer_avg_amount: f32,
    pub tx_count_24h: u32,
    pub mcc: u32,
    pub merchant_avg_amount: f32,
    pub is_online: bool,
    pub card_present: bool,
    pub km_from_home: f32,
    pub is_unknown_merchant: bool,
    pub has_last_tx: bool,
    pub minutes_since_last: u32,
    pub km_from_current: f32,
}

pub fn parse(buf: &[u8]) -> Option<Payload> {
    let tx = memmem::find(buf, b"\"transaction\"")?;
    let cust = memmem::find(buf, b"\"customer\"")?;
    let merch = memmem::find(buf, b"\"merchant\"")?;
    let term = memmem::find(buf, b"\"terminal\"")?;
    let last = memmem::find(buf, b"\"last_transaction\"")?;

    let mut offs = [(tx, 0u8), (cust, 1), (merch, 2), (term, 3), (last, 4)];
    offs.sort_unstable_by_key(|&(o, _)| o);
    let mut ranges = [(0usize, 0usize); 5];
    for i in 0..5 {
        let start = offs[i].0;
        let end = if i + 1 < 5 { offs[i + 1].0 } else { buf.len() };
        ranges[offs[i].1 as usize] = (start, end);
    }
    let tx_s = &buf[ranges[0].0..ranges[0].1];
    let cust_s = &buf[ranges[1].0..ranges[1].1];
    let merch_s = &buf[ranges[2].0..ranges[2].1];
    let term_s = &buf[ranges[3].0..ranges[3].1];
    let last_s = &buf[ranges[4].0..ranges[4].1];

    let amount = num_after(tx_s, b"\"amount\"")?;
    let installments = int_after(tx_s, b"\"installments\"")? as u8;
    let (req_y, req_mo, req_d, req_h, req_min, _req_s) = iso_after(tx_s, b"\"requested_at\"")?;

    let customer_avg_amount = num_after(cust_s, b"\"avg_amount\"")?;
    let tx_count_24h = int_after(cust_s, b"\"tx_count_24h\"")? as u32;

    let merchant_id = string_after(merch_s, b"\"id\"")?;
    let mcc_str = string_after(merch_s, b"\"mcc\"")?;
    let mcc = digits_to_u32(mcc_str);
    let merchant_avg_amount = num_after(merch_s, b"\"avg_amount\"")?;

    let is_online = bool_after(term_s, b"\"is_online\"")?;
    let card_present = bool_after(term_s, b"\"card_present\"")?;
    let km_from_home = num_after(term_s, b"\"km_from_home\"")?;

    let km_region = after_key(cust_s, b"\"known_merchants\"")?;
    let is_unknown_merchant = !array_contains(km_region, merchant_id);

    let last_val = after_key(last_s, b"\"last_transaction\"")?;
    let has_last_tx = !last_val.starts_with(b"null");
    let (minutes_since_last, km_from_current) = if has_last_tx {
        let (ly, lmo, ld, lh, lmin, _ls) = iso_after(last_s, b"\"timestamp\"")?;
        let km = num_after(last_s, b"\"km_from_current\"")?;
        let mins = minutes_between(ly, lmo, ld, lh, lmin, req_y, req_mo, req_d, req_h, req_min);
        (mins, km)
    } else {
        (0, 0.0)
    };

    Some(Payload {
        amount,
        installments,
        hour: req_h,
        day_of_week: day_of_week(req_y, req_mo, req_d),
        customer_avg_amount,
        tx_count_24h,
        mcc,
        merchant_avg_amount,
        is_online,
        card_present,
        km_from_home,
        is_unknown_merchant,
        has_last_tx,
        minutes_since_last,
        km_from_current,
    })
}

#[inline]
fn after_key<'a>(buf: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let p = memmem::find(buf, key)?;
    let mut i = p + key.len();
    while i < buf.len() {
        match buf[i] {
            b':' | b' ' | b'\t' | b'\n' | b'\r' => i += 1,
            _ => break,
        }
    }
    Some(&buf[i..])
}

#[inline]
fn num_after(buf: &[u8], key: &[u8]) -> Option<f32> {
    let rest = after_key(buf, key)?;
    Some(parse_f32(rest).0)
}

#[inline]
fn int_after(buf: &[u8], key: &[u8]) -> Option<i64> {
    let rest = after_key(buf, key)?;
    Some(parse_i64(rest).0)
}

#[inline]
fn bool_after(buf: &[u8], key: &[u8]) -> Option<bool> {
    let rest = after_key(buf, key)?;
    Some(rest.first()? == &b't')
}

#[inline]
fn string_after<'a>(buf: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
    let rest = after_key(buf, key)?;
    let q = memchr::memchr(b'"', rest)?;
    let tail = &rest[q + 1..];
    let end = memchr::memchr(b'"', tail)?;
    Some(&tail[..end])
}

#[inline]
fn iso_after(buf: &[u8], key: &[u8]) -> Option<(u16, u8, u8, u8, u8, u8)> {
    let rest = after_key(buf, key)?;
    let q = memchr::memchr(b'"', rest)?;
    let s = &rest[q + 1..];
    if s.len() < 20 {
        return None;
    }
    let y = (s[0] - b'0') as u16 * 1000
        + (s[1] - b'0') as u16 * 100
        + (s[2] - b'0') as u16 * 10
        + (s[3] - b'0') as u16;
    let mo = (s[5] - b'0') * 10 + (s[6] - b'0');
    let d = (s[8] - b'0') * 10 + (s[9] - b'0');
    let h = (s[11] - b'0') * 10 + (s[12] - b'0');
    let mi = (s[14] - b'0') * 10 + (s[15] - b'0');
    let se = (s[17] - b'0') * 10 + (s[18] - b'0');
    Some((y, mo, d, h, mi, se))
}

#[inline]
fn digits_to_u32(s: &[u8]) -> u32 {
    let mut v = 0u32;
    for &b in s {
        if b.is_ascii_digit() {
            v = v.wrapping_mul(10).wrapping_add((b - b'0') as u32);
        }
    }
    v
}

fn parse_f32(s: &[u8]) -> (f32, usize) {
    let mut pos = 0;
    let mut neg = false;
    if pos < s.len() && s[pos] == b'-' {
        neg = true;
        pos += 1;
    }
    let mut int_part: u64 = 0;
    while pos < s.len() && s[pos].is_ascii_digit() {
        int_part = int_part
            .wrapping_mul(10)
            .wrapping_add((s[pos] - b'0') as u64);
        pos += 1;
    }
    let mut v = int_part as f64;
    if pos < s.len() && s[pos] == b'.' {
        pos += 1;
        let frac_start = pos;
        let mut frac: u64 = 0;
        while pos < s.len() && s[pos].is_ascii_digit() {
            if pos - frac_start < 18 {
                frac = frac * 10 + (s[pos] - b'0') as u64;
            }
            pos += 1;
        }
        let digits = (pos - frac_start).min(18);
        let scale = 10f64.powi(-(digits as i32));
        v += frac as f64 * scale;
    }

    if pos < s.len() && (s[pos] == b'e' || s[pos] == b'E') {
        pos += 1;
        let mut esign = 1i32;
        if pos < s.len() && (s[pos] == b'+' || s[pos] == b'-') {
            if s[pos] == b'-' {
                esign = -1;
            }
            pos += 1;
        }
        let mut e = 0i32;
        while pos < s.len() && s[pos].is_ascii_digit() {
            e = e * 10 + (s[pos] - b'0') as i32;
            pos += 1;
        }
        v *= 10f64.powi(esign * e);
    }
    if neg {
        v = -v;
    }
    (v as f32, pos)
}

fn parse_i64(s: &[u8]) -> (i64, usize) {
    let mut pos = 0;
    let mut neg = false;
    if pos < s.len() && s[pos] == b'-' {
        neg = true;
        pos += 1;
    }
    let mut v: i64 = 0;
    while pos < s.len() && s[pos].is_ascii_digit() {
        v = v * 10 + (s[pos] - b'0') as i64;
        pos += 1;
    }
    if neg {
        v = -v;
    }
    (v, pos)
}

fn day_of_week(y: u16, m: u8, d: u8) -> u8 {
    const T: [u16; 12] = [0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4];
    let ya = if m < 3 { (y - 1) as u32 } else { y as u32 };
    let dow = (ya + ya / 4 - ya / 100 + ya / 400 + T[(m - 1) as usize] as u32 + d as u32) % 7;
    ((dow + 6) % 7) as u8
}

fn days_since_epoch(y: i32, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y / 400 } else { (y - 399) / 400 };
    let yoe = (y - era * 400) as u32;
    let mm = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mm + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era as i64 * 146097 + doe as i64 - 719468
}

fn minutes_between(
    y1: u16,
    mo1: u8,
    d1: u8,
    h1: u8,
    mi1: u8,
    y2: u16,
    mo2: u8,
    d2: u8,
    h2: u8,
    mi2: u8,
) -> u32 {
    let d1 = days_since_epoch(y1 as i32, mo1 as u32, d1 as u32);
    let d2 = days_since_epoch(y2 as i32, mo2 as u32, d2 as u32);
    let m1 = d1 * 1440 + (h1 as i64) * 60 + (mi1 as i64);
    let m2 = d2 * 1440 + (h2 as i64) * 60 + (mi2 as i64);
    (m2 - m1).max(0) as u32
}

fn array_contains(buf: &[u8], needle: &[u8]) -> bool {
    let end = memchr::memchr(b']', buf).unwrap_or(buf.len());
    let region = &buf[..end];
    let mut i = 0;
    while i < region.len() {
        if region[i] == b'"' {
            let start = i + 1;
            if let Some(off) = memchr::memchr(b'"', &region[start..]) {
                let s = &region[start..start + off];
                if s == needle {
                    return true;
                }
                i = start + off + 1;
            } else {
                break;
            }
        } else {
            i += 1;
        }
    }
    false
}
