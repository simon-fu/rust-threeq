use pretty_hex::{hex_write, HexConfig};

pub fn hex_line_write<W: std::fmt::Write, T: AsRef<[u8]>>(
    f: &mut W,
    source: T,
    max_len: usize,
) -> std::fmt::Result {
    let source = source.as_ref();
    let v = if source.len() <= max_len {
        source
    } else {
        &source[..max_len]
    };

    let cfg = HexConfig {
        title: false,
        width: 0,
        group: 0,
        ..HexConfig::default()
    };
    // write!(f, "|")?;
    write!(f, "|bin({})|", source.len())?;
    // write!(f, "|")?;
    hex_write(f, &v, cfg)?;
    write!(f, "|")?;
    if source.len() > 16 {
        write!(f, "..")?;
    }
    write!(f, "|")
}

pub fn str_or_hex_line_write<W: std::fmt::Write, T: AsRef<[u8]>>(
    f: &mut W,
    source: T,
    max_len: usize,
) -> std::fmt::Result {
    let source = source.as_ref();
    if source.len() == 0 {
        return hex_line_write(f, source, max_len);
    }

    if let Ok(s) = std::str::from_utf8(source) {
        write!(f, "|str({})|{}|", source.len(), s)
        // write!(f, "|str({})|{}|", source.len(), s)?;
        // if source.len() > 16 {
        //     write!(f, "..")?;
        // }
        // write!(f, "|")
    } else {
        hex_line_write(f, source, max_len)
    }
}

pub struct StrOrHexLine<'a, T: 'a>(&'a T, usize);

impl<'a, T: 'a + AsRef<[u8]>> std::fmt::Display for StrOrHexLine<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        str_or_hex_line_write(f, self.0, self.1)
    }
}

impl<'a, T: 'a + AsRef<[u8]>> std::fmt::Debug for StrOrHexLine<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        str_or_hex_line_write(f, self.0, self.1)
    }
}

pub struct HexLine<'a, T: 'a>(&'a T, usize);

impl<'a, T: 'a + AsRef<[u8]>> std::fmt::Display for HexLine<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        hex_line_write(f, self.0, self.1)
    }
}

impl<'a, T: 'a + AsRef<[u8]>> std::fmt::Debug for HexLine<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        hex_line_write(f, self.0, self.1)
    }
}

pub trait BinStrLine: Sized {
    fn bin_str(&self) -> StrOrHexLine<Self>;
    fn bin_str_limit(&self, max: usize) -> StrOrHexLine<Self>;
    fn dump_bin(&self) -> HexLine<Self>;
    fn dump_bin_limit(&self, max: usize) -> HexLine<Self>;
}

impl<T> BinStrLine for T
where
    T: AsRef<[u8]>,
{
    fn bin_str(&self) -> StrOrHexLine<Self> {
        StrOrHexLine(self, 16)
    }

    fn bin_str_limit(&self, max: usize) -> StrOrHexLine<Self> {
        StrOrHexLine(self, max)
    }

    fn dump_bin(&self) -> HexLine<Self> {
        HexLine(self, 16)
    }

    fn dump_bin_limit(&self, max: usize) -> HexLine<Self> {
        HexLine(self, max)
    }
}
