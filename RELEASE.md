# Release Process

This document describes how to create a new release of the rechtspraak-extractor package.

## Overview

The package uses Git tags for version management. When you push a tag that starts with `v` (e.g., `v1.5.3`), the CI/CD pipeline automatically:

1. Builds the package
2. Publishes it to PyPI
3. Creates a GitHub release with the built artifacts

## Creating a Release

### Step 1: Ensure your changes are committed and pushed

```bash
git add .
git commit -m "Your changes"
git push origin rechtspraak
```

### Step 2: Create and push a version tag

```bash
# Create a tag (e.g., v1.5.3)
git tag v1.5.3

# Push the tag to trigger the release workflow
git push origin v1.5.3
```

The version number should follow [Semantic Versioning](https://semver.org/):
- MAJOR version for incompatible API changes
- MINOR version for new functionality in a backward compatible manner
- PATCH version for backward compatible bug fixes

### Step 3: Monitor the GitHub Actions workflow

1. Go to the [Actions tab](https://github.com/maastrichtlawtech/rechtspraak-extractor/actions) in the repository
2. Watch the workflow run complete
3. Verify the package was published to PyPI
4. Check that a GitHub release was created

## Version Management

The package uses `setuptools_scm` to automatically determine the version from Git tags:

- **On tagged commits**: The version will be the tag name (without the `v` prefix)
- **Between releases**: The version will be something like `1.5.3.dev10+g1234567` indicating development builds

The version is automatically written to `rechtspraak_extractor/_version.py` during the build process.

## Testing Releases

For testing purposes, pull requests automatically publish to TestPyPI. You can install from TestPyPI to test before making an official release:

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ rechtspraak_extractor
```

## Troubleshooting

### Build fails on tag push

1. Check the GitHub Actions logs for specific errors
2. Ensure the tag follows the `v*.*.*` format
3. Verify that all required GitHub secrets are configured (PyPI publishing credentials)

### Wrong version number

If the version number is incorrect:
1. Delete the tag locally: `git tag -d v1.5.3`
2. Delete the tag remotely: `git push origin :refs/tags/v1.5.3`
3. Create and push the correct tag
