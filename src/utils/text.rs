use std::cmp::Ordering;

use pinyin::ToPinyin;

pub fn compare_phonetic(a: &str, b: &str) -> Ordering {
    natord::compare(&to_phonetic(a), &to_phonetic(b))
}

pub fn is_ascii_digits(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit())
}

fn to_phonetic(text: &str) -> String {
    let mut s = String::new();

    text.chars().for_each(|c| {
        if matches!(
            c,
            '\u{3400}'..='\u{9FFF}' |
            '\u{20000}'..='\u{2EE5D}' |
            '\u{30000}'..='\u{323AF}'
        ) {
            let pinyin = c
                .to_pinyin()
                .iter()
                .map(|p| p.with_tone_num())
                .collect::<Vec<_>>()
                .join(" ");
            s.push_str(&pinyin);
        } else {
            s.push(c);
        }
    });

    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_phonetic() {
        let mut texts = ["你好", "世界", "こんにち", "저는", "Hello", "world"];
        texts.sort_by(|a, b| compare_phonetic(&a, &b));

        assert_eq!(
            texts,
            ["Hello", "你好", "世界", "world", "こんにち", "저는"]
        );
    }

    #[test]
    fn test_is_ascii_digits() {
        assert!(is_ascii_digits("1234567890"));
        assert!(!is_ascii_digits(""));
        assert!(!is_ascii_digits("xyz"));
        assert!(!is_ascii_digits("123.xyz"));
    }
}
