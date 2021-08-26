# Releases of emulators/storage

This document provides instructions for building a release of `emulators/storage`.

The release process consists of a handful of tasks:
1. Drop a release tag in git.
2. Creates a release in GitHub and creates provisional release notes (in the form of a change log).
3. Build a docker image for the new release.
4. Push the docker image to Docker Hub, with both a version tag and the "latest" tag.

Most of this is automated via a script in this same directory. The main thing you will need is a GitHub personal access token, which will be used for creating the release in GitHub (so you need write access to the fullstorydev/emulators repo).

## Creating a new release

So, to actually create a new release, just run the script in this directory.

First, you need a version number for the new release, following sem-ver format: `v<Major>.<Minor>.<Patch>`. Second, you need a personal access token for GitHub.

```sh
# from the root of the package
GITHUB_TOKEN=<Token> ./releasing/do-release.sh v<Major>.<Minor>.<Patch>
```

Wasn't that easy! There is one last step: update the release notes in GitHub. By default, the script just records a change log of commit descriptions. Use that log (and, if necessary, drill into individual PRs included in the release) to flesh out notes in the format of the `RELEASE_NOTES.md` file _in this directory_. Then login to GitHub, go to the new release, edit the notes, and paste in the markdown you just wrote.

That should be all there is to it!
