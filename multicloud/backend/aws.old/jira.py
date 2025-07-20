from jira import JIRA
import keyring
import json

class JiraAPI(JIRA):
    def __init__(self):
        """Initializes the JIRA API using a token generated in your Jira account profile.   The token should be stored in your
        keyring using a shell-command like:

        keyring set pge jira <<!
        <paste-token-here>
        !

        In addition JiraAPI looks for your LANID information in the pge user secret
        """
        jira_token = keyring.get_password('pge','jira')
        lanid = json.loads(keyring.get_password('pge','user'))['lanid']
        super().__init__(
            server="https://jirapge.atlassian.net",     
            basic_auth=(f'{lanid}@pge.com', jira_token)
        )
            

class Project:
    def __init__(self, id):
        self.jira = JiraAPI()
        self.id = id
        self.project = self.jira.project(id)
        self.boards = self.jira.boards(projectKeyOrID=id)
        self.board = self.boards[0]
        self._sprints = None

    @property
    def sprints(self):
        if self._sprints is None:
            self._sprints = SprintList(self)
        return self._sprints

    @property
    def active_sprint(self):
        return self.sprints.active_sprint()

    @property
    def previous_sprint(self):
        return self.sprints.previous_sprint()
    
    def next_sprint(self, sprint):
        return self.sprints.next_sprint(sprint)

class SprintList:
    def __init__(self, project : Project):
        self.jira = project.jira
        self.project = project
        all_sprints = []
        while True:
            sprints = self.jira.sprints(project.board.id, startAt=len(all_sprints))
            all_sprints.extend(sprints)
            if len(sprints)<50:
                break
        all_sprints.sort(key=lambda x: x.startDate)
        self.sprints = all_sprints

    def previous_sprint(self, sprint=None):
        if sprint is None:
            sprint = self.active_sprint()
        previous_needle = None
        for needle in self.sprints:
            if needle.id == sprint.id:
                return Sprint(self, previous_needle)
            previous_needle = needle
        raise ValueError(f"Sprint {sprint} not found in this project")

    def active_sprint(self):
        actives = [ s for s in self.sprints if s.state == 'active' ]
        if len(actives) != 1:
            if len(actives) == 0:
                raise RuntimeError("No active sprint")
            elif len(actives) > 1:
                raise NotImplementedError("Unimplemented: More than one active sprint")
        return Sprint(self, actives[0])
    
    def next_sprint(self, sprint=None):
        if sprint is None:
            sprint = self.active_sprint()
        sprints = self.sprints
        for index, needle in enumerate(sprints):
            if needle.id == sprint.id:
                if index+1 < len(sprints):
                    return Sprint(self, sprints[index+1])
                else:
                    return None
        raise ValueError(f"Sprint {sprint} not found in this project")


class Sprint:
    def __init__(self, sprintlist : SprintList, sprintdata):
        self.jira = sprintlist.jira
        self.sprintlist = sprintlist
        self.sprint = sprintdata

    @property
    def id(self):
        return self.sprint.id

    def __repr__(self):
        return f"{self.sprint.name}"
        
    def issues(self):
        all_issues = []
        while True:
            issues = self.jira.search_issues(f'project = {self.sprintlist.project.id} & sprint = {self.sprint.id}', startAt=len(all_issues))
            all_issues.extend(issues)
            if len(issues)<50: break
        return IssueList(self, all_issues)


class IssueList:
    def __init__(self, sprint : Sprint, issues):
        self.issues = issues
        self.jira = sprint.jira
        self.sprint = sprint

    def tech_debt(self):
        return IssueList(self.sprint, [ i for i in self.issues if 'dai-tech-debt' in i.fields.labels ])

    def __iter__(self):
        return IssueListIterator(self)

    def __getitem__(self, index):
        return Issue(self.jira, self.issues[index])

    def story_points(self, status=None):
        """calculates the total story points in a list of issues

        status :str: If specified then only issues that have the specified status (e.g. 'Done'( will be included in the total.
                     By default all issues are included.
        """
        sum = 0
        for i in self:
            if status and not str(i.data['status']) == str(status):
                continue
            story_points = i.data['story_points']
            if story_points:
                sum += story_points
        return sum


class Issue:
    def __init__(self, jira : JiraAPI, issuedata):
        self.jira = jira
        self.issue = issuedata
        self.data = {
            'funnel': self.issue.fields.customfield_10014,
            'summary': self.issue.fields.summary,
            'type': self.issue.fields.issuetype,
            'status': self.issue.fields.status,
            'assignee': self.issue.fields.assignee,
            'created': self.issue.fields.created,
            'story_points': self.issue.fields.customfield_10048,
            'labels': self.issue.fields.labels
        }

    def __repr__(self):
        str = f"[{self.issue.key}] {self.data['summary']}\n"
        for i in ['funnel', 'type', 'status', 'assignee', 'created', 'story_points', 'labels']:
            str += f"  {i}: {self.data[i]}\n"
        return str

class IssueListIterator:
    def __init__(self, issuelist):
        self.issuelist = issuelist
        self.index = 0

    def __next__(self):
        if self.index >= len(self.issuelist.issues):
            raise StopIteration()
        index = self.index
        self.index+=1
        return Issue(self.issuelist.jira, self.issuelist.issues[index])





    
