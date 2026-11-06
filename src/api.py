from contextlib import asynccontextmanager
import logging
import os
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from src.storage import StorageManager
from src.reconciler import ReconciliationEngine

logger = logging.getLogger(__name__)

# Instantiate storage and reconciler engine
storage_manager = StorageManager()
reconciliation_engine = ReconciliationEngine(storage_mgr=storage_manager)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Ensure AWS S3 and DynamoDB resources are initialized upon startup."""
    use_real_aws = os.getenv("USE_REAL_AWS", "false").lower() in ("true", "1")
    mock = None
    if not use_real_aws:
        from moto import mock_aws
        mock = mock_aws()
        mock.start()
        storage_manager.reconnect()
        logger.info("Running in Local Simulation Mode with in-memory AWS S3 & DynamoDB.")

    try:
        storage_manager.ensure_resources_exist()
    except Exception as exc:
        logger.warning(f"Resource provisioning note: {exc}")

    yield

    if mock:
        mock.stop()


app = FastAPI(
    title="Amazon OpsPulse: Workflow & Data Reconciliation Engine",
    description="Internal operational service for automated data validation, state tracking in DynamoDB, and audit reports in S3.",
    version="1.0.0",
    lifespan=lifespan,
)


class TransactionItem(BaseModel):
    transaction_id: str
    order_id: str
    amount: float
    expected_amount: float
    currency: str = "INR"
    status: str = "CAPTURED"
    account_id: Optional[str] = "ACT-IND-01"


class TriggerJobRequest(BaseModel):
    batch_name: Optional[str] = "Manual-Ops-Trigger"
    records: List[TransactionItem]


@app.get("/", response_class=HTMLResponse, tags=["Dashboard"])
def get_dashboard():
    """Serves the OpsPulse Interactive Operations Dashboard."""
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Amazon OpsPulse | Cloud Workflow Engine</title>
        <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
        <style>
            .state-pill { transition: all 0.3s ease; }
            .active-state { background-color: #10B981; color: white; transform: scale(1.05); }
        </style>
    </head>
    <body class="bg-gray-900 text-gray-100 min-h-screen">
        <div class="max-w-6xl mx-auto py-10 px-4">
            <!-- Header -->
            <div class="flex flex-col md:flex-row justify-between items-start md:items-center pb-6 border-b border-gray-800">
                <div>
                    <h1 class="text-3xl font-extrabold text-white tracking-tight flex items-center">
                        <span class="text-yellow-400 mr-2">⚡</span> Amazon OpsPulse
                    </h1>
                    <p class="text-gray-400 text-sm mt-1">Cloud-Native Operational Task Automation & Reconciliation Service</p>
                </div>
                <div class="mt-4 md:mt-0 flex space-x-3 items-center">
                    <span class="px-3 py-1 bg-yellow-900 text-yellow-300 rounded-full text-xs font-semibold">AWS S3: Connected</span>
                    <span class="px-3 py-1 bg-blue-900 text-blue-300 rounded-full text-xs font-semibold">DynamoDB: Active</span>
                    <a href="/docs" target="_blank" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white text-xs font-bold rounded-lg shadow transition">
                        Open Swagger API Docs ↗
                    </a>
                </div>
            </div>

            <!-- Action Controls -->
            <div class="mt-8 bg-gray-800 p-6 rounded-xl border border-gray-700 shadow-xl">
                <div class="flex flex-col md:flex-row justify-between items-center">
                    <div>
                        <h2 class="text-lg font-bold text-white">Trigger Batch Reconciliation Workflow</h2>
                        <p class="text-sm text-gray-400">Processes 5 batch transaction records across accounts, detecting amount discrepancies and streaming audit logs.</p>
                    </div>
                    <button id="runBtn" onclick="runWorkflow()" class="mt-4 md:mt-0 px-6 py-3 bg-gradient-to-r from-yellow-500 to-yellow-600 hover:from-yellow-600 hover:to-yellow-700 text-gray-900 font-bold rounded-lg shadow-lg flex items-center transition transform active:scale-95">
                        <span>🚀 Run Workflow Batch</span>
                    </button>
                </div>

                <!-- State Machine Stepper -->
                <div class="mt-8 pt-6 border-t border-gray-700">
                    <p class="text-xs uppercase tracking-wider text-gray-400 mb-3 font-semibold">State Machine Lifecycle (FSM)</p>
                    <div class="grid grid-cols-4 gap-3 text-center text-xs font-bold">
                        <div id="st-pending" class="state-pill py-2 rounded-lg bg-gray-700 text-gray-300">1. PENDING</div>
                        <div id="st-running" class="state-pill py-2 rounded-lg bg-gray-700 text-gray-300">2. RUNNING</div>
                        <div id="st-validating" class="state-pill py-2 rounded-lg bg-gray-700 text-gray-300">3. VALIDATING</div>
                        <div id="st-completed" class="state-pill py-2 rounded-lg bg-gray-700 text-gray-300">4. COMPLETED</div>
                    </div>
                </div>
            </div>

            <!-- Results Section (hidden initially) -->
            <div id="resultsSection" class="hidden mt-8 space-y-6">
                <!-- Summary Metrics Grid -->
                <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <div class="bg-gray-800 p-4 rounded-xl border border-gray-700">
                        <p class="text-xs text-gray-400 uppercase font-semibold">Total Records</p>
                        <p id="res-total" class="text-2xl font-black text-white mt-1">0</p>
                    </div>
                    <div class="bg-gray-800 p-4 rounded-xl border border-gray-700">
                        <p class="text-xs text-gray-400 uppercase font-semibold">Clean Records</p>
                        <p id="res-clean" class="text-2xl font-black text-green-400 mt-1">0</p>
                    </div>
                    <div class="bg-gray-800 p-4 rounded-xl border border-gray-700">
                        <p class="text-xs text-gray-400 uppercase font-semibold">Discrepancies</p>
                        <p id="res-disc" class="text-2xl font-black text-red-400 mt-1">0</p>
                    </div>
                    <div class="bg-gray-800 p-4 rounded-xl border border-gray-700">
                        <p class="text-xs text-gray-400 uppercase font-semibold">Net Variance</p>
                        <p id="res-variance" class="text-2xl font-black text-yellow-400 mt-1">0</p>
                    </div>
                </div>

                <!-- Cloud Persistence Status Card -->
                <div class="bg-gray-800 p-6 rounded-xl border border-gray-700">
                    <h3 class="text-sm font-bold text-gray-300 uppercase tracking-wider mb-2">Cloud Persistence Artifacts</h3>
                    <div class="space-y-2 text-xs font-mono">
                        <div class="flex items-center text-gray-300">
                            <span class="font-bold text-blue-400 w-32">Job ID:</span>
                            <span id="res-job-id" class="text-white"></span>
                        </div>
                        <div class="flex items-center text-gray-300">
                            <span class="font-bold text-yellow-400 w-32">AWS S3 Report:</span>
                            <span id="res-s3-url" class="text-yellow-200 break-all"></span>
                        </div>
                        <div class="flex items-center text-gray-300">
                            <span class="font-bold text-green-400 w-32">DynamoDB State:</span>
                            <span class="px-2 py-0.5 rounded bg-green-900 text-green-300 font-bold">COMPLETED (Stored)</span>
                        </div>
                    </div>
                </div>

                <!-- Discrepancy Items Table -->
                <div class="bg-gray-800 rounded-xl border border-gray-700 overflow-hidden">
                    <div class="px-6 py-4 border-b border-gray-700 flex justify-between items-center">
                        <h3 class="text-sm font-bold text-white uppercase tracking-wider">Identified Operational Discrepancies</h3>
                        <span class="px-2 py-1 bg-red-900 text-red-300 rounded text-xs font-bold">Requires Ops Review</span>
                    </div>
                    <div class="overflow-x-auto">
                        <table class="w-full text-left text-xs">
                            <thead class="bg-gray-900 text-gray-400 uppercase font-semibold">
                                <tr>
                                    <th class="py-3 px-4">Txn ID</th>
                                    <th class="py-3 px-4">Order ID</th>
                                    <th class="py-3 px-4">Settled Amount</th>
                                    <th class="py-3 px-4">Expected</th>
                                    <th class="py-3 px-4">Delta Variance</th>
                                    <th class="py-3 px-4">Anomaly Type</th>
                                </tr>
                            </thead>
                            <tbody id="discrepancyTableBody" class="divide-y divide-gray-700 text-gray-300 font-mono">
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>

        <script>
            const samplePayload = {
                batch_name: "Ops-Live-Web-Batch",
                records: [
                    { transaction_id: "TXN-1001", order_id: "ORD-90210", amount: 1499.00, expected_amount: 1499.00, currency: "INR", status: "CAPTURED", account_id: "ACT-IND-01" },
                    { transaction_id: "TXN-1002", order_id: "ORD-90211", amount: 2999.00, expected_amount: 2599.00, currency: "INR", status: "DISCREPANCY_OVERCHARGED", account_id: "ACT-IND-02" },
                    { transaction_id: "TXN-1003", order_id: "ORD-90212", amount: 499.00, expected_amount: 499.00, currency: "INR", status: "CAPTURED", account_id: "ACT-IND-01" },
                    { transaction_id: "TXN-1004", order_id: "ORD-90213", amount: 0.00, expected_amount: 1200.00, currency: "INR", status: "UNPAID_ZERO_VALUED", account_id: "ACT-IND-03" },
                    { transaction_id: "TXN-1005", order_id: "ORD-90214", amount: 850.50, expected_amount: 850.50, currency: "INR", status: "CAPTURED", account_id: "ACT-IND-02" }
                ]
            };

            function resetSteppers() {
                ['st-pending', 'st-running', 'st-validating', 'st-completed'].forEach(id => {
                    document.getElementById(id).className = "state-pill py-2 rounded-lg bg-gray-700 text-gray-300";
                });
            }

            function setStepActive(id) {
                document.getElementById(id).className = "state-pill py-2 rounded-lg active-state font-bold shadow-lg";
            }

            async function runWorkflow() {
                const btn = document.getElementById('runBtn');
                btn.disabled = true;
                btn.classList.add('opacity-50', 'cursor-not-allowed');

                resetSteppers();
                setStepActive('st-pending');
                await new Promise(r => setTimeout(r, 300));

                setStepActive('st-running');
                await new Promise(r => setTimeout(r, 400));

                setStepActive('st-validating');

                try {
                    const response = await fetch('/jobs/reconcile', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(samplePayload)
                    });
                    const data = await response.json();

                    await new Promise(r => setTimeout(r, 400));
                    setStepActive('st-completed');

                    // Display summary
                    document.getElementById('resultsSection').classList.remove('hidden');
                    document.getElementById('res-total').innerText = data.summary.total_records;
                    document.getElementById('res-clean').innerText = data.summary.clean_records;
                    document.getElementById('res-disc').innerText = data.summary.discrepancies_detected;
                    document.getElementById('res-variance').innerText = "₹" + data.summary.net_variance;
                    document.getElementById('res-job-id').innerText = data.job_id;
                    document.getElementById('res-s3-url').innerText = data.s3_report_url;

                    // Fetch the detailed report directly from S3 endpoint to populate table!
                    const reportResp = await fetch(`/jobs/${data.job_id}/report`);
                    const reportData = await reportResp.json();

                    const tbody = document.getElementById('discrepancyTableBody');
                    tbody.innerHTML = '';
                    (reportData.discrepancies || []).forEach(d => {
                        const tr = document.createElement('tr');
                        tr.innerHTML = `
                            <td class="py-3 px-4 font-bold text-white">${d.transaction_id}</td>
                            <td class="py-3 px-4 text-gray-400">${d.order_id}</td>
                            <td class="py-3 px-4 text-green-300">₹${d.amount.toFixed(2)}</td>
                            <td class="py-3 px-4 text-gray-300">₹${d.expected_amount.toFixed(2)}</td>
                            <td class="py-3 px-4 ${d.discrepancy_delta > 0 ? 'text-red-400' : 'text-yellow-400'} font-bold">
                                ${d.discrepancy_delta > 0 ? '+' : ''}₹${d.discrepancy_delta.toFixed(2)}
                            </td>
                            <td class="py-3 px-4">
                                <span class="px-2 py-0.5 rounded text-xs font-semibold ${d.issue_type === 'OVERCHARGED' ? 'bg-red-800 text-red-200' : 'bg-yellow-800 text-yellow-200'}">
                                    ${d.issue_type}
                                </span>
                            </td>
                        `;
                        tbody.appendChild(tr);
                    });

                } catch (err) {
                    alert('Error executing workflow: ' + err.message);
                } finally {
                    btn.disabled = false;
                    btn.classList.remove('opacity-50', 'cursor-not-allowed');
                }
            }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


@app.get("/health", tags=["Monitoring"])
def health_check():
    """Service health and AWS configuration status."""
    return {
        "status": "HEALTHY",
        "service": "OpsPulse Reconciliation Engine",
        "aws_region": storage_manager.region_name,
        "s3_bucket": storage_manager.s3_bucket_name,
        "dynamodb_table": storage_manager.dynamodb_table_name,
    }


@app.post("/jobs/reconcile", status_code=status.HTTP_201_CREATED, tags=["Workflows"])
def trigger_reconciliation_job(payload: TriggerJobRequest):
    """Triggers an automated workflow job to reconcile transaction records."""
    records_dict = [item.model_dump() for item in payload.records]
    try:
        result = reconciliation_engine.execute_job(
            records=records_dict,
            job_metadata={"batch_name": payload.batch_name},
        )
        return {
            "message": "Workflow completed successfully",
            "job_id": result["job_id"],
            "status": result["status"],
            "s3_report_url": result["s3_report_url"],
            "summary": result["summary"],
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Reconciliation job failed: {str(exc)}",
        )


@app.get("/jobs/{job_id}", tags=["Workflows"])
def get_job_status(job_id: str):
    """Queries job execution metadata and state from AWS DynamoDB."""
    record = storage_manager.get_job_execution(job_id)
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found in DynamoDB.",
        )
    return record


@app.get("/jobs/{job_id}/report", tags=["Reports"])
def get_job_s3_report(job_id: str):
    """Fetches the complete operational audit report from AWS S3."""
    record = storage_manager.get_job_execution(job_id)
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{job_id}' not found in DynamoDB.",
        )

    s3_url = record.get("s3_report_url")
    if not s3_url:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No S3 report associated with job '{job_id}'.",
        )

    try:
        report = storage_manager.download_audit_report(s3_url)
        return report
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch report from S3: {str(exc)}",
        )
