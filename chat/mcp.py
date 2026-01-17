import asyncio
from typing import Annotated, Sequence, TypedDict
from langchain_openai import ChatOpenAI
from langchain_core.messages import BaseMessage, HumanMessage
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent
from langchain_mcp_adapters.client import MultiServerMCPClient
import chromadb

# --- SETUP RAG TOOL ---
# This tool allows the agent to search your 28MB of transcripts
client = chromadb.PersistentClient(path="./charlotte_rag_db")
collection = client.get_collection(name="council_transcripts")

@tool
def search_council_transcripts(query: str):
    """Searches the Charlotte City Council meeting transcripts for specific keywords, 
    contracts, or vendor names mentioned in meetings."""
    results = collection.query(query_texts=[query], n_results=3)
    return "\n\n".join(results['documents'][0])

async def run_auditor():
    # 1. Connect to your local MCP Server (The investigator tool we wrote earlier)
    # Make sure your charlotte_audit_mcp.py is running or accessible
    async with MultiServerMCPClient({
        "charlotte-investigator": {
            "command": "python",
            "args": ["charlotte_audit_mcp.py"],
            "transport": "stdio"
        }
    }) as mcp_client:
        
        # 2. Pull the MCP tools (verify_vendor, etc.)
        mcp_tools = await mcp_client.get_tools()
        
        # 3. Combine RAG tool + MCP tools
        all_tools = [search_council_transcripts] + mcp_tools
        
        # 4. Initialize the LLM (Claude or GPT-4o)
        model = ChatOpenAI(model="gpt-4o", temperature=0)
        
        # 5. Create the Agent
        system_message = (
            "You are the Queen City Auditor. Your goal is to find discrepancies in Charlotte's "
            "government spending. Use 'search_council_transcripts' to find what was said in meetings. "
            "If a vendor or contract is mentioned, ALWAYS use your MCP tools to verify if they are "
            "actually registered in the city's live database."
        )
        agent = create_react_agent(model, all_tools, state_modifier=system_message)

        # 6. Test Query
        inputs = {"messages": [HumanMessage(content="Was there any mention of 'ABC Construction' in recent meetings, and are they a real vendor?")]}
        async for chunk in agent.astream(inputs, stream_mode="values"):
            final_result = chunk["messages"][-1].content
            
        print(f"\nAUDITOR REPORT:\n{final_result}")

if __name__ == "__main__":
    asyncio.run(run_auditor())


#     check_checkbook	City Checkbook	Follow the Money: Lets the agent look up actual payments made to a vendor to see if they match the contract amounts mentioned in the transcripts.
# get_rezoning_history	Zoning Variances	Identify Lobbying: Allows the AI to see if a company mentioned in a transcript also has active rezoning requests, helping flag potential conflicts of interest.
# code_enforcement_lookup	Code Enforcement Cases	Quality Check: If a developer is asking for a city contract, the agent can check if they have a history of code violations or "slumlord" complaints.