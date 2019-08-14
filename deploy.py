from os import path

from github import Github
import sys

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: deploy.py organization repository token commit tag [file1, file2, ...]")
        exit(-1)

    org_name = sys.argv[1]
    repo_name = sys.argv[2]
    token = sys.argv[3]
    tag_name = sys.argv[4]

    g = Github(token)
    repo = g.get_repo(org_name + "/" + repo_name)

    tags = repo.get_tags()
    deploy_tag = None
    for tag in tags:
        if tag.name == tag_name:
            deploy_tag = tag

    if deploy_tag is None:
        print("No tag found")
        exit(0)

    print(deploy_tag)

    releases = repo.get_releases()
    deploy_release = None
    for release in releases:
        if release.tag_name == tag_name:
            deploy_release = release
    print(deploy_release)

    if deploy_release is None:
        deploy_release = repo.create_git_release(tag_name, tag_name, "")

    for i in range(5, len(sys.argv)):
        deploy_release.upload_asset(sys.argv[i], path.basename(sys.argv[i]))




