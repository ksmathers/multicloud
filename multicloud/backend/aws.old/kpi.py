from jira import Project
from generic_templates import Arglist

class KPI_Lagging_TechDebtServicedLastSprint:
    def __init__(self, project):
        if type(project) is str:
            project = Project(project)
        self.project = project

    def value(self):
        sprint = self.project.previous_sprint
        issues = sprint.issues()
        techdebt = issues.tech_debt()
        pct = techdebt.story_points('Done') / issues.story_points('Done') * 100
        score = pct / 20 * 10
        return score
    
class KPI_Leading_TechDebtScheduledActiveFuture:
    def __init__(self, project):
        if type(project) is str:
            project = Project(project)
        self.project = project

    def value(self):
        issues_pts = 0
        techdebt_pts = 0
        sprint = self.project.active_sprint
        while sprint is not None:
            issues = sprint.issues()
            techdebt = issues.tech_debt()
            issues_pts += issues.story_points()
            techdebt_pts += techdebt.story_points()
            sprint = self.project.next_sprint(sprint)

        pct = techdebt_pts / issues_pts * 100
        score = pct / 20 * 10
        return score
    
KPI = {
    "Leading_TechDebtScheduledActiveFuture": { "class": KPI_Leading_TechDebtScheduledActiveFuture, "kws": "project"},
    "Lagging_TechDebtServicedLastSprint": { "class": KPI_Lagging_TechDebtServicedLastSprint, "kws": "project"},
}

