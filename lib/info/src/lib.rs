use const_format::{concatcp, formatcp};

const GIT_SHA: &str = match option_env!("GIT_SHA") {
    Some(sha) => sha,
    None => "unknown",
};

const BUILD_TIME: &str = match option_env!("BUILD_TIME") {
    Some(time) => time,
    None => "unknown",
};

pub const fn git_sha() -> &'static str {
    GIT_SHA
}

pub const fn short_git_sha() -> &'static str {
    let bytes = GIT_SHA.as_bytes();
    let len = if bytes.len() < 7 { bytes.len() } else { 7 };
    unsafe { std::str::from_utf8_unchecked(bytes.split_at(len).0) }
}

pub const fn build_time() -> &'static str {
    BUILD_TIME
}

pub const fn version() -> &'static str {
    formatcp!(
        "chutils {} (commit: {GIT_SHA}) built on {BUILD_TIME}",
        env!("CARGO_PKG_VERSION"),
    )
}

pub const fn user_agent() -> &'static str {
    concatcp!("chutils/", env!("CARGO_PKG_VERSION"), "-", short_git_sha())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn git_sha_returns_static_str() {
        let sha = git_sha();
        assert!(!sha.is_empty());
    }

    #[test]
    fn short_git_sha_is_at_most_7_chars() {
        let short = short_git_sha();
        assert!(short.len() <= 7);
    }

    #[test]
    fn short_git_sha_is_prefix_of_full_sha() {
        let full = git_sha();
        let short = short_git_sha();
        assert!(full.starts_with(short));
    }

    #[test]
    fn short_git_sha_unknown_is_7_chars() {
        // When GIT_SHA is "unknown" (7 chars), short_git_sha returns all of it
        if git_sha() == "unknown" {
            assert_eq!(short_git_sha(), "unknown");
        }
    }

    #[test]
    fn build_time_returns_static_str() {
        let time = build_time();
        assert!(!time.is_empty());
    }

    #[test]
    fn version_contains_package_version() {
        let v = version();
        assert!(
            v.contains(env!("CARGO_PKG_VERSION")),
            "version string should contain the crate version"
        );
    }

    #[test]
    fn version_contains_commit_sha() {
        let v = version();
        assert!(
            v.contains(git_sha()),
            "version string should contain the full git sha"
        );
    }

    #[test]
    fn version_contains_build_time() {
        let v = version();
        assert!(
            v.contains(build_time()),
            "version string should contain the build time"
        );
    }

    #[test]
    fn version_has_expected_format() {
        let v = version();
        assert!(v.starts_with("chutils "));
        assert!(v.contains("(commit: "));
        assert!(v.contains("built on "));
    }

    #[test]
    fn user_agent_has_expected_format() {
        let ua = user_agent();
        let expected_prefix = format!("chutils/{}-", env!("CARGO_PKG_VERSION"));
        assert!(
            ua.starts_with(&expected_prefix),
            "user_agent '{ua}' should start with '{expected_prefix}'"
        );
    }

    #[test]
    fn user_agent_ends_with_short_sha() {
        let ua = user_agent();
        assert!(
            ua.ends_with(short_git_sha()),
            "user_agent should end with the short git sha"
        );
    }
}
