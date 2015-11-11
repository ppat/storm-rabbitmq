# How to release to Maven Central

* Follow Central recommendations for [setting up Apache Maven][ossrh-maven]
* Ensure GPG is set up for signing jars
* Ensure `~/.m2/settings.xml` has the following contents for signing
  and uploading jars to Maven Central

```
<settings>
  <servers>
    <server>
      <id>sonatype-nexus-staging</id>
      <username>[OSS_USER]</username>
      <password>[OSS_PASSWORD]</password>
    </server>
  </servers>
</settings>

```

### Prepare for release checklist

[ ] Run unit testing and check test coverage

[ ] Ensure Javadoc API is fully documented

[ ] Update README.md for release

### Perform release

1. Increment the version in `pom.xml` to a stable release:
   e.g. `0.7`.  Commit.
2. Tag the current commit: e.g. `0.7`.
3. Upload the release.
   ```
   mvn -e clean deploy -P sonatype-oss-release
   ```
4. Increment plugin version to next snapshot: e.g. `0.8-SNAPSHOT`.  Commit.
5. Push commits and tags
   ```
   git push origin && git push --tags origin
   ```

This will initially upload the artifact to a staging repository.  Once confident
about the release visit [Maven Central Nexus][ossrh] and follow [instructions on
releasing to production][ossrh-release].

[ossrh-maven]: http://central.sonatype.org/pages/apache-maven.html
[ossrh-guide]: http://central.sonatype.org/pages/ossrh-guide.html
[ossrh]: https://oss.sonatype.org/
[ossrh-release]: http://central.sonatype.org/pages/releasing-the-deployment.html
