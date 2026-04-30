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
    let mut p = 0usize;

    skip_through(&mut p, buf, b"\"amount\"")?;
    let amount = scan_f32(&mut p, buf);

    skip_through(&mut p, buf, b"\"installments\"")?;
    let installments = scan_u32(&mut p, buf) as u8;

    skip_through(&mut p, buf, b"\"requested_at\"")?;
    let (req_y, req_mo, req_d, req_h, req_min) = scan_iso(&mut p, buf)?;

    skip_through(&mut p, buf, b"\"avg_amount\"")?;
    let customer_avg_amount = scan_f32(&mut p, buf);

    skip_through(&mut p, buf, b"\"tx_count_24h\"")?;
    let tx_count_24h = scan_u32(&mut p, buf);

    skip_through(&mut p, buf, b"\"known_merchants\"")?;
    skip_to(&mut p, buf, b'[')?;
    p += 1;
    let km_start = p;
    skip_to(&mut p, buf, b']')?;
    let km_end = p;
    p += 1;

    skip_through(&mut p, buf, b"\"id\"")?;
    let merchant_id = scan_string(&mut p, buf)?;

    skip_through(&mut p, buf, b"\"mcc\"")?;
    let mcc = scan_mcc(&mut p, buf);

    skip_through(&mut p, buf, b"\"avg_amount\"")?;
    let merchant_avg_amount = scan_f32(&mut p, buf);

    skip_through(&mut p, buf, b"\"is_online\"")?;
    let is_online = scan_bool(&mut p, buf);

    skip_through(&mut p, buf, b"\"card_present\"")?;
    let card_present = scan_bool(&mut p, buf);

    skip_through(&mut p, buf, b"\"km_from_home\"")?;
    let km_from_home = scan_f32(&mut p, buf);

    skip_through(&mut p, buf, b"\"last_transaction\"")?;
    skip_colon(&mut p, buf);
    skip_ws(&mut p, buf);

    let has_last_tx = p < buf.len() && buf[p] != b'n';
    let (minutes_since_last, km_from_current) = if has_last_tx {
        skip_through(&mut p, buf, b"\"timestamp\"")?;
        let (ly, lmo, ld, lh, lmin) = scan_iso(&mut p, buf)?;
        skip_through(&mut p, buf, b"\"km_from_current\"")?;
        let km = scan_f32(&mut p, buf);
        let mins = minutes_between(ly, lmo, ld, lh, lmin, req_y, req_mo, req_d, req_h, req_min);
        (mins, km)
    } else {
        (0, 0.0)
    };

    let is_unknown_merchant = !array_contains(&buf[km_start..km_end], merchant_id);

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
fn skip_through(p: &mut usize, buf: &[u8], needle: &[u8]) -> Option<()> {
    let pos = memchr::memmem::find(&buf[*p..], needle)?;
    *p += pos + needle.len();
    Some(())
}

#[inline]
fn skip_to(p: &mut usize, buf: &[u8], byte: u8) -> Option<()> {
    let pos = memchr::memchr(byte, &buf[*p..])?;
    *p += pos;
    Some(())
}

#[inline]
fn skip_colon(p: &mut usize, buf: &[u8]) {
    while *p < buf.len() && buf[*p] != b':' {
        *p += 1;
    }
    if *p < buf.len() {
        *p += 1;
    }
}

#[inline]
fn skip_ws(p: &mut usize, buf: &[u8]) {
    while *p < buf.len() && matches!(buf[*p], b' ' | b'\t' | b'\n' | b'\r') {
        *p += 1;
    }
}

#[inline]
fn skip_to_value(p: &mut usize, buf: &[u8]) {
    while *p < buf.len() && matches!(buf[*p], b':' | b' ' | b'\t' | b'\n' | b'\r') {
        *p += 1;
    }
}

#[inline]
fn scan_f32(p: &mut usize, buf: &[u8]) -> f32 {
    skip_to_value(p, buf);
    let (v, len) = parse_f32(&buf[*p..]);
    *p += len;
    v
}

#[inline]
fn scan_u32(p: &mut usize, buf: &[u8]) -> u32 {
    skip_to_value(p, buf);
    let mut v = 0u32;
    while *p < buf.len() && buf[*p].is_ascii_digit() {
        v = v.wrapping_mul(10).wrapping_add((buf[*p] - b'0') as u32);
        *p += 1;
    }
    v
}

#[inline]
fn scan_bool(p: &mut usize, buf: &[u8]) -> bool {
    skip_to_value(p, buf);
    let result = *p < buf.len() && buf[*p] == b't';
    while *p < buf.len() && buf[*p].is_ascii_alphabetic() {
        *p += 1;
    }
    result
}

#[inline]
fn scan_string<'a>(p: &mut usize, buf: &'a [u8]) -> Option<&'a [u8]> {
    skip_to_value(p, buf);
    if *p >= buf.len() || buf[*p] != b'"' {
        return None;
    }
    *p += 1;
    let start = *p;
    let end = memchr::memchr(b'"', &buf[start..])?;
    *p = start + end + 1;
    Some(&buf[start..start + end])
}

#[inline]
fn scan_mcc(p: &mut usize, buf: &[u8]) -> u32 {
    skip_to_value(p, buf);
    if *p < buf.len() && buf[*p] == b'"' {
        *p += 1;
    }
    let mut v = 0u32;
    while *p < buf.len() && buf[*p].is_ascii_digit() {
        v = v.wrapping_mul(10).wrapping_add((buf[*p] - b'0') as u32);
        *p += 1;
    }
    if *p < buf.len() && buf[*p] == b'"' {
        *p += 1;
    }
    v
}

#[inline]
fn scan_iso(p: &mut usize, buf: &[u8]) -> Option<(u16, u8, u8, u8, u8)> {
    skip_to_value(p, buf);
    if *p < buf.len() && buf[*p] == b'"' {
        *p += 1;
    }
    let s = &buf[*p..];
    if s.len() < 19 {
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
    *p += 20;
    Some((y, mo, d, h, mi))
}

fn array_contains(buf: &[u8], needle: &[u8]) -> bool {
    let mut i = 0;
    while i < buf.len() {
        if buf[i] == b'"' {
            let start = i + 1;
            match memchr::memchr(b'"', &buf[start..]) {
                Some(off) => {
                    if &buf[start..start + off] == needle {
                        return true;
                    }
                    i = start + off + 1;
                }
                None => break,
            }
        } else {
            i += 1;
        }
    }
    false
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
        v += frac as f64 * 10f64.powi(-(digits as i32));
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
    let m1 = d1 * 1440 + h1 as i64 * 60 + mi1 as i64;
    let m2 = d2 * 1440 + h2 as i64 * 60 + mi2 as i64;
    (m2 - m1).max(0) as u32
}