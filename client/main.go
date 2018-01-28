package main

import (
	"cloud.google.com/go/bigquery"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

var (
	bqDatasetName = flag.String("bq_dataset_name", "", "BigQuery dataset name")
	bqProjectID   = flag.String("bq_project_id", "", "BigQuery project ID")
	linksInBody   = flag.Bool("links_in_body", true, "whether the download")
	outputPrefix  = flag.String("output_prefix", "", "prefix prepended to the default file name.")
	url           = flag.String("url", "", "url to fetching the bulk data from")

	resourceTypes = []string{
		"Account",
		"ActivityDefinition",
		"AdverseEvent",
		"AllergyIntolerance",
		"Appointment",
		"AppointmentResponse",
		"AuditEvent",
		"Basic",
		"Binary",
		"BodySite",
		"Bundle",
		"CapabilityStatement",
		"CarePlan",
		"CareTeam",
		"ChargeItem",
		"Claim",
		"ClaimResponse",
		"ClinicalImpression",
		"CodeSystem",
		"Communication",
		"CommunicationRequest",
		"CompartmentDefinition",
		"Composition",
		"ConceptMap",
		"Condition",
		"Consent",
		"Contract",
		"Coverage",
		"DataElement",
		"DetectedIssue",
		"Device",
		"DeviceComponent",
		"DeviceMetric",
		"DeviceRequest",
		"DeviceUseStatement",
		"DiagnosticReport",
		"DocumentManifest",
		"DocumentReference",
		"EligibilityRequest",
		"EligibilityResponse",
		"Encounter",
		"Endpoint",
		"EnrollmentRequest",
		"EnrollmentResponse",
		"EpisodeOfCare",
		"ExpansionProfile",
		"ExplanationOfBenefit",
		"FamilyMemberHistory",
		"Flag",
		"Goal",
		"GraphDefinition",
		"Group",
		"GuidanceResponse",
		"HealthcareService",
		"ImagingManifest",
		"ImagingStudy",
		"Immunization",
		"ImmunizationRecommendation",
		"ImplementationGuide",
		"Library",
		"Linkage",
		"List",
		"Location",
		"Measure",
		"MeasureReport",
		"Media",
		"Medication",
		"MedicationAdministration",
		"MedicationDispense",
		"MedicationRequest",
		"MedicationStatement",
		"MessageDefinition",
		"MessageHeader",
		"NamingSystem",
		"NutritionOrder",
		"Observation",
		"OperationDefinition",
		"OperationOutcome",
		"Organization",
		"Parameters",
		"Patient",
		"PaymentNotice",
		"PaymentReconciliation",
		"Person",
		"PlanDefinition",
		"Practitioner",
		"PractitionerRole",
		"Procedure",
		"ProcedureRequest",
		"ProcessRequest",
		"ProcessResponse",
		"Provenance",
		"Questionnaire",
		"QuestionnaireResponse",
		"ReferralRequest",
		"RelatedPerson",
		"RequestGroup",
		"ResearchStudy",
		"ResearchSubject",
		"RiskAssessment",
		"Schedule",
		"SearchParameter",
		"Sequence",
		"ServiceDefinition",
		"Slot",
		"Specimen",
		"StructureDefinition",
		"StructureMap",
		"Subscription",
		"Substance",
		"SupplyDelivery",
		"SupplyRequest",
		"Task",
		"TestReport",
		"TestScript",
		"ValueSet",
		"VisionPrescription",
	}
)

func reqBulkData(url string) (string, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Add("Accept", "application/fhir+ndjson")
	req.Header.Add("Prefer", "respond-async")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 202 {
		return "", fmt.Errorf("got status \"%v\", want 202", resp.Status)
	}
	return resp.Header.Get("Content-Location"), nil
}

func getLinksFromHeader(resp *http.Response) []string {
	ret := []string{}
	links := strings.Split(resp.Header.Get("Link"), ",")
	for _, link := range links {
		ret = append(ret, strings.Trim(link, "<>"))
	}
	return ret
}

func unmarshalForLinks(body json.RawMessage) ([]string, error) {
	var jb map[string]interface{}
	if err := json.Unmarshal(body, &jb); err != nil {
		return []string{}, fmt.Errorf("unmarshal body: %v", err)
	}
	output, ok := jb["output"]
	if !ok {
		return []string{}, fmt.Errorf("field \"output\" not found in response body")
	}
	array, ok := output.([]interface{})
	if !ok {
		return []string{}, fmt.Errorf("unmarshal output array")
	}
	ret := []string{}
	for _, element := range array {
		config := element.(map[string]interface{})
		ret = append(ret, config["url"].(string))
	}
	return ret, nil
}

func getLinksFromBody(resp *http.Response) ([]string, error) {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []string{}, err
	}
	return unmarshalForLinks(body)
}

func getBulkDataLinks(url string) ([]string, error) {
	// TODO: add timeout.
	for {
		resp, err := http.Get(url)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		switch resp.StatusCode {
		case 200:
			if *linksInBody {
				links, err := getLinksFromBody(resp)
				if err != nil {
					return links, err
				} else {
					return links, nil
				}
			} else {
				return getLinksFromHeader(resp), nil
			}
		case 202:
			fmt.Println("Not ready. Sleeping 5 seconds...")
			time.Sleep(5 * time.Second)
		default:
			return []string{}, fmt.Errorf("got status %v, want 200", resp.Status)
		}
	}
}

func fetchBody(url string) ([]byte, error) {
	resp, err := http.Get(url)
	if err != nil {
		return []byte{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return []byte{}, fmt.Errorf("got status \"%v\", want 200", resp.Status)
	}
	if app := resp.Header.Get("Content-Type"); !strings.Contains(app, "application/fhir+ndjson") {
		return []byte{}, fmt.Errorf("expect content type application/fhir+ndjson, got %v", app)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return []byte{}, err
	}
	return body, nil
}

func extractFilename(path string) string {
	lastIndex := strings.LastIndex(path, "/")
	if lastIndex < 0 || lastIndex >= len(path)-1 {
		return path
	}
	return path[lastIndex+1:]
}

func extractResourceType(s string) string {
	for _, t := range resourceTypes {
		if strings.Contains(s, t) {
			return t
		}
	}
	return ""
}

func main() {
	flag.Parse()
	cl, err := reqBulkData(*url)
	if err != nil {
		log.Fatalf("failed to request bulk data from %v: %v", *url, err)
	}
	fmt.Printf("content location: %v\n", cl)
	links, err := getBulkDataLinks(cl)
	if err != nil {
		log.Fatalf("failed to get bulk data links from %v: %v", cl, err)
	}
	var dataset *bigquery.Dataset = nil
	ctx := context.Background()
	if *bqProjectID != "" {
		client, err := bigquery.NewClient(ctx, *bqProjectID)
		if err != nil {
			log.Println("For authentication issues, remember to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.")
			log.Fatalf("Failed to create BigQuery client: %v", err)
		}
		dataset = client.Dataset(*bqDatasetName)
		if dataset == nil {
			log.Fatalf("dataset not found: %v", *bqDatasetName)
		}
	}
	for _, link := range links {
		fmt.Printf("Fetching %v...", link)
		body, err := fetchBody(link)
		if err != nil {
			log.Fatalf(" FAILED\n")
		}
		// Save to file if output_prefix is specified.
		name := extractFilename(link)
		if *outputPrefix != "" {
			fmt.Printf(" Writing to %v...", name)
			if err := ioutil.WriteFile(*outputPrefix+name, body, 0660); err != nil {
				fmt.Printf(" FAILED\n")
				log.Fatalf("failed to download %v to %v: %v", link, name, err)
			}
		}
		// Load to BigQuery if a dataset exists.
		if dataset != nil {
			tableName := extractResourceType(name)
			if tableName == "" {
				fmt.Printf(" FAILED\n")
				log.Fatalf("found no valid resource name in %v", name)
			}
			fmt.Printf(" Loading to BigQuery table %v...", tableName)
			table := dataset.Table(tableName)
			if err := table.Create(ctx, nil); err != nil {
				fmt.Printf(" Table may already exist...")
			}
			_, err := table.Metadata(ctx)
			if err != nil {
				fmt.Printf(" FAILED\n")
				log.Fatalf("failed to get metadata for %v: %v", tableName, err)
			}
		}
		fmt.Printf(" Done\n")
	}
}
