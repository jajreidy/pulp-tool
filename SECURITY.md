# Security Policy

## Supported Versions

We actively support the following versions with security updates:

| Version | Supported          |
| ------- | ------------------ |
| 1.0.x   | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

We take security vulnerabilities seriously. If you discover a security vulnerability, please follow these steps:

1. **Do NOT** open a public GitHub issue
2. Email security details to: **rokartifactstorage@redhat.com**
3. Include:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if any)

### What to Expect

- We will acknowledge receipt within 48 hours
- We will provide an initial assessment within 7 days
- We will keep you informed of progress
- We will coordinate disclosure after a fix is available

### Responsible Disclosure

We follow responsible disclosure practices:

- We will work with you to understand and resolve the issue
- We will credit you in security advisories (unless you prefer otherwise)
- We will not take legal action against security researchers acting in good faith

## Security Best Practices

### Authentication

- Never commit credentials or API keys to the repository
- Use environment variables or secure config files for sensitive data
- Rotate credentials regularly
- Use OAuth2 tokens with appropriate scopes

### Dependencies

- Keep dependencies up-to-date
- Review security advisories for dependencies
- Use `pip audit` or similar tools to check for vulnerabilities

### Configuration

- Use secure file permissions for config files (600 for keys)
- Validate all user input
- Use HTTPS for all API communications
- Verify SSL certificates

### Code Practices

- Follow secure coding practices
- Validate and sanitize all inputs
- Use parameterized queries/requests
- Avoid exposing sensitive information in logs

## Security Updates

Security updates will be:

- Released as patch versions (e.g., 1.0.1)
- Documented in CHANGELOG.md
- Tagged with security labels in GitHub
- Announced via GitHub security advisories

## Known Security Considerations

### OAuth2 Token Storage

- Tokens are stored in memory only
- Tokens are refreshed proactively to avoid expiration
- No tokens are logged or persisted

### Certificate Handling

- Client certificates are loaded from files
- Certificate paths are validated
- SSL verification is enabled by default

### File Operations

- File paths are validated before operations
- File permissions are checked
- Large files are handled with streaming

## Additional Resources

- [GitHub Security Advisories](https://github.com/konflux/pulp-tool/security/advisories)
- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [Python Security Best Practices](https://python.readthedocs.io/en/stable/library/security_warnings.html)
