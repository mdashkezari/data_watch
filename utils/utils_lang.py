
import pandas as pd
import language_tool_python



def language_check(text):
    """
    detect grammar errors and spelling mistakes.
    """
    text = str(text)
    lTool = language_tool_python.LanguageTool('en-US')
    matches = lTool.check(text)

    results = pd.DataFrame()
    for match in matches:
        row = pd.DataFrame([{
            "ruleID": match.ruleId,
            "message": match.message,
            "sentence": str(match.sentence),
            "replacements": str(match.replacements),
            "offsetInContext": str(match.offsetInContext),
            "context": str(match.context),
            "offset": str(match.offset),
            "errorLength": str(match.errorLength),
            "category": str(match.category),
            "ruleIssueType": str(match.ruleIssueType)            
        }])
        results = pd.concat([results, row], ignore_index=True)
    return results

