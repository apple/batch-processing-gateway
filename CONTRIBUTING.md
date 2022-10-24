# Contributing

Thank you for your interest in contributing! By joining force we can make great products.

### How Can I Make Contribution?

#### Reporting Bugs

Please report bugs by creating [Github issues](https://docs.github.com/en/issues/tracking-your-work-with-issues/about-issues).
To help the community understand the bug and get it fixed faster, please provide the following information when creating a new issue:
- A clear and descriptive title
- The K8s cluster setup and the app config
- The exact steps to reproduce the bug
- The observed behavior and expected behavior
- The urgency of the issue (scale of 1 - 4, with 1 being the most urgent)

If possible, also include payloads, commands, screenshots, etc to help the community identify the problem. Do not include any personal or sensitive data.


#### Suggesting Improvements

You can suggest improvements also by creating Github issues.
When creating a new suggestion, please provide the following information:
- A clear and descriptive title
- A description of the proposed improvement in as many details as possible
- Explain why the improvement is important

#### Documentation Contribution

Documentation contribution will make it easier for the community to work on the project.
You may add README/diagrams to the components, or improve the existing docs. For major doc changes, we encourage you to create issues before contributing. Let us know what you are planning to change before the contribution. 


#### Code Contribution

See the [Code Contribution](#code-contribution) section for more details.


## Code Contribution

### New Features & Bug Fixes

For minor changes (like small bug fixes or typo correction), feel free to open up a PR directly.
For new features or major changes, we encourage you to create a Github issue first, and get agreement from community leaders before starting on the implementation. This is to save you time in case there's duplicate effort or unforeseen risk.


### Pull Request
When creating a pull request, please follow the template provided. It will help you reflect on your changes and make sure the contribution is well understood by the reviewers. 

## Code Style

The Java code base follow the [Google Java Style](https://google.github.io/styleguide/javaguide.html).
A [pre-commit](https://pre-commit.com) config is included to add [google-java-formatter v1.15.0](https://github.com/google/google-java-format/releases/tag/v1.15.0) as a pre-commit hook.
Enabling this can make Git reformat the code changes automatically according to the code style before committing.

To enable the pre-commit hook:
- Install pre-commit by following [the instruction](https://pre-commit.com)
- Run this from the repo root directory: `$ pre-commit install`


## Additional Notes

- [Code of Conduct](./CODE_OF_CONDUCT.md)
- [License](./LICENSE)
