<!--
*Thank you for contributing.
To help others review your PR in the best possible way, please go through the checklist below,
which will get your PR into a shape in which it can be best reviewed.*

*Please understand that we do not do this to make PR submission a hassle. 
In order to uphold a high standard of quality for code contributions, while at the same time keeping track
of the development work, we need PR authors to prepare the PRs well, and give reviewers enough contextual information for the review. *

## Contribution Checklist

  - Name the pull request in the form "[Component] A one-liner summary of the change".
    You may skip the [Component] if you are unsure about which is the best component.

  - Fill out the template below to describe the changes contributed by the pull request. That will give reviewers the context they need to do the review.
  
  - Make sure that the change passes the automated tests (TO BE ADDED).

  - Each pull request should address only one issue, not mix up code from multiple issues.
  
  - Each commit in the pull request has a meaningful commit message. Include issue links if possible.

  - Remove the above text and this checklist, leaving only the filled out template below.
-->

## What is the Purpose of the Change

*(Please briefly describe the purpose of the change and leave the details in the change log section below)*


## Type of Change
<!-- Put an "X" in the [ ] as [ X ] to mark the selection -->

- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] This change requires a documentation update


## Brief Change Log

*(Please list out all the changed items)*


## Verifying This Change

*(Please pick either of the following options)*

This change is a trivial rework / code cleanup without any test coverage.

*(or)*

This change is already covered by existing tests, such as *(please describe tests)*.

*(or)*

This change added tests and can be verified as follows:

*(example:)*
  - *Added integration tests for end-to-end deployment with large payloads (100MB)*
  - *Extended integration test for recovery after master failure*
  - *Added test that validates that TaskInfo is transferred only once across recoveries*
  - *Manually verified the change by running a 4 node cluser*

## Documentation

  - Does this pull request introduce a new feature? (yes / no)
  - If yes, how is the feature documented? (not applicable / docs / JavaDocs / not documented)


# Final Checklist:
<!-- Put an "X" in the [ ] as [ X ] to mark the selection -->

- [ ] My code follows the style guidelines of this project
- [ ] I have performed a self-review of my own code
- [ ] I have commented my code, particularly in hard-to-understand areas
- [ ] I have made corresponding changes to the documentation
- [ ] My changes generate no new warnings
- [ ] I have added tests that prove my fix is effective or that my feature works
- [ ] New and existing unit tests pass locally with my changes
- [ ] Any dependent changes have been merged and published in downstream modules
