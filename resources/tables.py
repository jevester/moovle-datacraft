import json

import pandas as pd

def parsingDate(date):
    if date == '0001-01-01':
        return None
    else:
        return date

def parsingDateTime(v):
    return pd.to_datetime(v)

class Tables:
    def wmsDocumentLines(resp):
        data = []
        for e in resp:
            data.append({
                'id': e['id'],
                'documentType': e['documentType'],
                'documentNo': e['documentNo'],
                'lineNo': e['lineNo'],
                'sellToCustomerNo': e['sellToCustomerNo'],
                'sellToCustomerNo2': e['sellToCustomerNo2'],
                'sellToCustomerName': e['sellToCustomerName'],
                'buyFromVendorNo': e['buyFromVendorNo'],
                'buyFromVendorName': e['buyFromVendorName'],
                'dossierNo': e['dossierNo'],
                'carrierTypeCode': e['carrierTypeCode'],
                'currencyCode': e['currencyCode'],
                'currencyFactor': e['currencyFactor'],
                'lineAmountLCY': e['lineAmountLCY'],
                'type': e['type'],
                'no': e['no'],
                'externalNo': e['externalNo'],
                'itemNo': e['itemNo'],
                'description': e['description'],
                'description2': e['description2'],
                'baseUnitOfMeasureCode': e['baseUnitOfMeasureCode'],
                'unitOfMeasureCode': e['unitOfMeasureCode'],
                'qtyPerUnitOfMeasure': e['qtyPerUnitOfMeasure'],
                'quantity': e['quantity'],
                'quantityBase': e['quantityBase'],
                'qtyCreated': e['qtyCreated'],
                'qtyOutstanding': e['qtyOutstanding'],
                'qtyPosted': e['qtyPosted'],
                'qtyBaseCreated': e['qtyBaseCreated'],
                'qtyBasePosted': e['qtyBasePosted'],
                'qtyBaseOutstanding': e['qtyBaseOutstanding'],
                'storageChargeNo': e['storageChargeNo'],
                'unitPrice': e['unitPrice'],
                'lineAmount': e['lineAmount'],
                'carrierQuantity': e['carrierQuantity'],
                'carrierQtyCreated': e['carrierQtyCreated'],
                'carrierQtyPosted': e['carrierQtyPosted'],
                'qtyPerCarrier': e['qtyPerCarrier'],
                'defaultNetWeightPerUOM': e['defaultNetWeightPerUOM'],
                'carrierQtyOutstanding': e['carrierQtyOutstanding'],
                'createdDateTime': parsingDateTime(e['createdDateTime']),
                'createdUserID': e['createdUserID'],
                'modifiedDateTime': parsingDateTime(e['modifiedDateTime']),
                'modifiedUserID': e['modifiedUserID'],
                'orderUnitOfMeasureCode': e['orderUnitOfMeasureCode'],
                'qtyPerOrderUnitOfMeasure': e['qtyPerOrderUnitOfMeasure'],
                'tareWeightPerUoM': e['tareWeightPerUoM'],
                'grossWeightPerUoM': e['grossWeightPerUoM'],
                'netWeightPerUoM': e['netWeightPerUoM'],
                'volumeWeight': e['volumeWeight'],
                'tareWeight': e['tareWeight'],
                'grossWeight': e['grossWeight'],
                'netWeight': e['netWeight'],
                'grossWeightCreated': e['grossWeightCreated'],
                'netWeightCreated': e['netWeightCreated'],
                'batchNo': e['batchNo'],
                'externalBatchNo': e['externalBatchNo'],
                'grossWeightPosted': e['grossWeightPosted'],
                'netWeightPosted': e['netWeightPosted'],
                'grossWeightOutstanding': e['grossWeightOutstanding'],
                'netWeightOutstanding': e['netWeightOutstanding'],
                'qtyToHandle': e['qtyToHandle'],
                'customerItemNo2': e['customerItemNo2'],
                'batchNo2': e['batchNo2'],
                'storageChargeNo2': e['storageChargeNo2'],
                'unitOfMeasureCode2': e['unitOfMeasureCode2'],
                'locationNo': e['locationNo'],
                'buildingCode': e['buildingCode'],
                'customsCode': e['customsCode'],
                'tariffNo': e['tariffNo'],
                'countryOfOriginCode': e['countryOfOriginCode'],
                'countryPurchasedCode': e['countryPurchasedCode'],
                'customsValue': e['customsValue'],
                'customsValuePer': e['customsValuePer'],
                'nctsDocumentNo': e['nctsDocumentNo'],
                'additionalDocumentNo': e['additionalDocumentNo'],
                'placeOfCertificateDelivery': e['placeOfCertificateDelivery'],
                'destination': e['destination'],
                'tariffDescription': e['tariffDescription'],
                'declarationDocumentType': e['declarationDocumentType'],
                'customsCurrencyCode': e['customsCurrencyCode'],
                'containerNo': e['containerNo'],
                'vesselNo': e['vesselNo'],
                'itemHandling': e['itemHandling'],
                'orderTypeCode': e['orderTypeCode'],
                'entrepotStorage': e['entrepotStorage'],
                'commentText': e['commentText'],
                'externalDocumentNo': e['externalDocumentNo'],
                'shortcutDimension1Code': e['shortcutDimension1Code'],
                'shortcutDimension2Code': e['shortcutDimension2Code'],
                'postingDate': parsingDate(e['postingDate']),
                'invoiceType': e['invoiceType'],
                'invoiceNo': e['invoiceNo'],
                'invoiceDate': parsingDate(e['invoiceDate']),
                'purchInvoiceNo': e['purchInvoiceNo'],
                'purchInvoiceDate': parsingDate(e['purchInvoiceDate']),
                'reservationPosted': e['reservationPosted'],
                'agreementType': e['agreementType'],
                'agreementNo': e['agreementNo'],
                'agreementLineNo': e['agreementLineNo'],
                'agreementDetailLineNo': e['agreementDetailLineNo'],
                'agreementDetailLineType': e['agreementDetailLineType'],
                'senderAddressNo': e['senderAddressNo'],
                'isActivity': e['isActivity'],
                'shipToAddressNo': e['shipToAddressNo'],
                'activityDate': parsingDate(e['activityDate']),
                'activityTime': e['activityTime'],
                'shipToAddressName': e['shipToAddressName'],
                'customsIssueDate': parsingDate(e['customsIssueDate']),
                'declarationDocumentNo': e['declarationDocumentNo'],
                'attribute01': e['attribute01'],
                'attribute02': e['attribute02'],
                'attribute03': e['attribute03'],
                'attribute04': e['attribute04'],
                'attribute05': e['attribute05'],
                'isUTB': True if e['no'] in ['UTB', 'BTW', 'BTWB02'] else False
            })
        return pd.DataFrame(data)


    def generalLedgerEntries(resp):
        data = []
        for e in resp:
            data.append({
                'id': e['id'],
                'entryNo': e['entryNo'],
                'gLAccountNo': e['gLAccountNo'],
                'postingDate': parsingDate(e['postingDate']),
                'documentType': e['documentType'],
                'documentNo': e['documentNo'],
                'description': e['description'],
                'balAccountNo': e['balAccountNo'],
                'amount': e['amount'],
                'globalDimension1Code': e['globalDimension1Code'],
                'globalDimension2Code': e['globalDimension2Code'],
                'userID': e['userID'],
                'sourceCode': e['sourceCode'],
                'priorYearEntry': e['priorYearEntry'],
                'quantity': e['quantity'],
                'journalBatchName': e['journalBatchName'],
                'genPostingType': e['genPostingType'],
                'genBusPostingGroup': e['genBusPostingGroup'],
                'debitAmount': e['debitAmount'],
                'creditAmount': e['creditAmount'],
                'documentDate': parsingDate(e['documentDate']),
                'externalDocumentNo': e['externalDocumentNo'],
                'sourceType': e['sourceType'],
                'sourceNo': e['sourceNo'],
                'reversedByEntryNo': e['reversedByEntryNo'],
                'reversedEntryNo': e['reversedEntryNo'],
                'gLAccountName': e['gLAccountName'],
                'shortcutDimension3Code': e['shortcutDimension3Code'],
                'shortcutDimension4Code': e['shortcutDimension4Code'],
                'lastModifiedDateTime': parsingDateTime(e['lastModifiedDateTime']),
                'wmsDocumentNo': e['wmsDocumentNo'] if e['wmsDocumentNo'] != '' else None,
                'wmsDocumentLineNo': e['wmsDocumentLineNo'] if e['wmsDocumentLineNo'] != '' else None,
                'noSeries': e['noSeries'],
                'isUTB': False if e['wmsDocumentNo'] == '' else None,
                'isCorrectieOfJaarafsluiting': True if ((int(e['gLAccountNo']) in [1392,1393,1650,1652]) and (len(e['wmsDocumentNo']) < 2) and (e['noSeries'] == 'FIN-DOC')) else False
            })
        return pd.DataFrame(data)


    def wmsDocumentHeaders(resp):
        data = []
        for e in resp:
            data.append({
                'id': e['id'],
                'documentType': e['documentType'],
                'no': e['no'],
                'sellToCustomerNo': e['sellToCustomerNo'],
                'buildingCode': e['buildingCode'],
                'postingDate': parsingDate(e['postingDate']),
                'statusCode': e['statusCode'],
                'containerNo': e['containerNo'],
                'vesselNo': e['vesselNo'],
                'senderAddressNo': e['senderAddressNo'],
                'shipToAddressNo': e['shipToAddressNo'],
                'billOfLadingNo': e['billOfLadingNo'],
                'shortcutDimension2Code': e['shortcutDimension2Code'],
                'attribute04': e['attribute04'],
                'containerSizeCode': e['containerSizeCode'],
                'sellToCustomerName': e['sellToCustomerName'],
                'billToCustomerNo': e['billToCustomerNo'],
                'billToCustomerName': e['billToCustomerName'],
                'salespersonCode': e['salespersonCode'],
                'direction': e['direction'],
                'locationNo': e['locationNo'],
                'movementType': e['movementType'],
                'documentDate': parsingDate(e['documentDate']),
                'orderDate': parsingDate(e['orderDate']),
                'statusTemplateCode': e['statusTemplateCode'],
                'createdDateTime': parsingDateTime(e['createdDateTime']),
                'modifiedDateTime': parsingDateTime(e['modifiedDateTime']),
                'noSeries': e['noSeries'],
                'externalDocumentNo': e['externalDocumentNo'],
                'externalReference': e['externalReference'],
                'shippingAgentCode': e['shippingAgentCode'],
                'announcedDate': parsingDate(e['announcedDate']),
                'arrivedDate': parsingDate(e['arrivedDate']),
                'departedDate': parsingDate(e['departedDate']),
                'deliveryDate': parsingDate(e['deliveryDate']),
                'estimatedDepartureDate': parsingDate(e['estimatedDepartureDate']),
                'parentDocumentType': e['parentDocumentType'],
                'parentDocumentNo': e['parentDocumentNo'],
                'customsCode': e['customsCode'],
                'tariffNo': e['tariffNo'],
                'countryOfOriginCode': e['countryOfOriginCode'],
                'countryOfDestinationCode': e['countryOfDestinationCode'],
                'declarationDate': parsingDate(e['declarationDate']),
                'destination': e['destination'],
                'expectQtyCarriers': e['expectQtyCarriers'],
                'customsValue': e['customsValue'],
                'incotermsCode': e['incotermsCode'],
                'invoiceValue': e['invoiceValue'],
                'incotermsCity': e['incotermsCity'],
                'additionalDocumentNo': e['additionalDocumentNo'],
                'certificateNo': e['certificateNo'],
                'shortcutDimension1Code': e['shortcutDimension1Code'],
                'attribute05': e['attribute05'],
                'grossWeight': e['grossWeight'],
                'netWeight': e['netWeight'],
                'quantity': e['quantity'],
                'carrierQuantity': e['carrierQuantity'],
                'portFrom': e['portFrom'],
                'portTo': e['portTo'],
                'sealNo': e['sealNo'],
                'sendersAddressName': e['sendersAddressName'],
                'shipToAddressName': e['shipToAddressName'],
                'sendersAddressCity': e['sendersAddressCity'],
                'shipToAddressCity': e['shipToAddressCity'],
                'carrierQtyCreated': e['carrierQtyCreated'],
                'consigneeName': e['consigneeName'],
                'shipperName': e['shipperName'],
                'agentName': e['agentName']
            })
        return pd.DataFrame(data)

    def generalLedgerAccounts(resp):
        data = []
        for e in resp:
            data.append({
                'id': e['id'],
                'no': e['no'],
                'no2': e['no2'],
                'name': e['name'],
                'incomeBalance': e['incomeBalance'],
                'accountCategory': e['accountCategory'],
                'accountSubcategoryEntryNo': e['accountSubcategoryEntryNo'],
                'accountSubcategoryDescription': e['accountSubcategoryDescript'],
                'debitCredit': e['debitCredit'],
                'accountType': e['accountType'],
                'totaling': e['totaling'],
                'balance': e['balance'],
                'lastModifiedDateTime': parsingDateTime(e['lastModifiedDateTime']),
                'genPostingType': e['genPostingType'],
                'vatProdPostingGroup': e['vatProdPostingGroup']

            })
        return pd.DataFrame(data)

    def customerLedgerEntries(resp):
        data = []
        for e in resp:
            data.append({
                'id': e['id'],
                'entryNo': e['entryNo'],
                'customerNo': e['customerNo'],
                'postingDate': parsingDate(e['postingDate']),
                'documentNo': e['documentNo'],
                'description': e['description'],
                'originalAmtLCY': e['originalAmtLCY'],
                'remainingAmtLCY': e['remainingAmtLCY'],
                'customerPostingGroup': e['customerPostingGroup'],
                'open': e['open'],
                'dueDate': parsingDate(e['dueDate']),

            })
        return pd.DataFrame(data)

    def vendorLedgerEntries(resp):
        data = []
        for e in resp:
            data.append({
                'id': e['id'],
                'entryNo': e['entryNo'],
                'vendorNo': e['vendorNo'],
                'postingDate': parsingDate(e['postingDate']),
                'documentNo': e['documentNo'],
                'description': e['description'],
                'originalAmtLCY': e['originalAmtLCY'],
                'remainingAmtLCY': e['remainingAmtLCY'],
                'amountLCY': e['amountLCY'],
                'purchaseLCY': e['purchaseLCY'],
                'open': e['open'],
                'dueDate': parsingDate(e['dueDate']),

            })
        return pd.DataFrame(data)


    def wmsDocumentContainers(resp):
        data = []
        for e in resp:
            data.append({
                'id': e['id'],
                "documentType": e['documentType'],
                "documentNo": e['documentNo'],
                "lineNo": e['lineNo'],
                "containerNo": e['containerNo'],
                "sizeCode": e['sizeCode'],
                "loadingAddressNo": e['loadingAddressNo'],
                "loadingReference": e['loadingReference'],
                "loadingAddressName": e['loadingAddressName'],
                "loadingAddressCity": e['loadingAddressCity'],
                "unloadingAddressNo": e['unloadingAddressNo'],
                "unloadingReference": e['unloadingReference'],
                "unloadingAddressName": e['unloadingAddressName'],
                "unloadingAddressCity": e['unloadingAddressCity'],
                "pickUpAddressNo": e['pickUpAddressNo'],
                "pickUpReference": e['pickUpReference'],
                "pickUpAddressName": e['pickUpAddressName'],
                "pickUpAddressCity": e['pickUpAddressCity'],
                "dropOffAddressNo": e['dropOffAddressNo'],
                "dropOffReference": e['dropOffReference'],
                "dropOffAddressName": e['dropOffAddressName'],
                "dropOffAddressCity": e['dropOffAddressCity'],
                "loadingDateFrom": parsingDate(e['loadingDateFrom']),
                "loadingDateTo": parsingDate(e['loadingDateTo']),
                "loadingTimeFrom": e['loadingTimeFrom'],
                "loadingTimeTo": e['loadingTimeTo'],
                "unloadingDateFrom": parsingDate(e['unloadingDateFrom']),
                "unloadingDateTo": parsingDate(e['unloadingDateTo']),
                "unloadingTimeFrom": e['unloadingTimeFrom'],
                "unloadingTimeTo": e['unloadingTimeTo'],
                "inDemurrageDate": parsingDate(e['inDemurrageDate']),
                "inDetentionDate": parsingDate(e['inDetentionDate']),
                "shippingCompanyAddress": e['shippingCompanyAddress'],
                "shippingCompanyName": e['shippingCompanyName'],
                "etdDate": parsingDate(e['etdDate']),
                "etdTime": e['etdTime'],
                "etaDate": parsingDate(e['etaDate']),
                "etaTime": e['etaTime'],
                "vesselNo": e['vesselNo'],
                "vesselname": e['vesselname'],
                "billOfLadingNo": e['billOfLadingNo']
            })
        return pd.DataFrame(data)

    def wmsPostedDocumentHeaders(resp):
        data = []
        for e in resp:
            data.append({
                'id': e['id'],
                "documentType": e['documentType'],
                "no": e['no'],
                'sellToCustomerNo': e['sellToCustomerNo'],
                "sellToCustomerName": e['sellToCustomerName'],
                "billToCustomerName": e['billToCustomerName'],
                "buildingCode": e['buildingCode'],
                "salespersonCode": e['salespersonCode'],
                "direction": e['direction'],
                "documentDate": parsingDate(e['documentDate']),
                "postingDate": parsingDate(e['postingDate']),
                "statusCode": e['statusCode'],
                "modifiedDateTime": parsingDateTime(e['modifiedDateTime']),
                "deliveryDate": parsingDate(e['deliveryDate']),
                "customsCode": e['customsCode'],
                "sendersAddressName": e['sendersAddressName'],
                "shipToAddressName": e['shipToAddressName'],
                "shortcutDimension2Code": e['shortcutDimension2Code'],
                "attribute04": e['attribute04'],
                "attribute05": e['attribute05']

            })
        return pd.DataFrame(data)

    def wmsPostedDocumentLines(resp):
        data = []
        for e in resp:
            data.append({
                'id': e['id'],
                'documentNo': e['documentNo'],
                'lineNo': e['lineNo'],
                'sellToCustomerNo': e['sellToCustomerNo'],
                'lineAmountLCY': e['lineAmountLCY'],
                'no': e['no'],
                'description': e['description'],
                'quantity': e['quantity'],
                'modifiedDateTime': parsingDateTime(e['modifiedDateTime']),
                'grossWeight': e['grossWeight'],
                'netWeight': e['netWeight'],
                'containerNo': e['containerNo'],
                'vesselNo': e['vesselNo'],
                'postingDate': parsingDate(e['postingDate']),
                'isUTB': True if e['no'] in ['UTB', 'BTW', 'BTWB02'] else False

            })
        return pd.DataFrame(data)

    def wmsDocumentPackageLines(resp):
        data = []
        for e in resp:
            data.append({
                'id': e['id'],
                'documentType': e['documentType'],
                'documentNo': e['documentNo'],
                'documentLineNo': e['documentLineNo'],
                'packageCode': e['packageCode'],
                'numberOfPackages': e['numberOfPackages'],
                'description': e['description'],
                'cubage': e['cubage'],
                'grossWeight': e['grossWeight'],
                'containerLineNo': e['containerLineNo'],
                'containerNo': e['containerNo'],
                'modifiedDateTime': parsingDateTime(e['modifiedDateTime'])

            })
        return pd.DataFrame(data)

    def wmsAddresses(resp):
        data = []
        for e in resp:
            data.append({
                'id': e['id'],
                "no": e['no'],
                "name": e['name'],
                "address": e['address'],
                "city": e['city'],
                "countryRegionName": e['countryRegionName'],
                "modifiedDateTime": parsingDateTime(e['modifiedDateTime'])

            })
        return pd.DataFrame(data)

    def wmsDocumentCommentLines(resp):
        data = []
        for e in resp:
            data.append({
                'id': e['id'],
                'documentType': e['documentType'],
                'documentNo': e['documentNo'],
                'documentLineNo': e['documentLineNo'],
                'lineNo': e['lineNo'],
                'date': e['date'],
                'code': e['code'],
                'comment': e['comment'],
                'instruction': e['instruction'],
                'source': e['source']
            })
        return pd.DataFrame(data)

    def wmsBuildings(resp):
        data = []
        for e in resp:
            data.append({
                'id': e['id'],
                'code': e['code'],
                'name': e['name'],
                'addressNo': e['addressNo'],
                'conditionSetID': e['conditionSetID'],
                'modifiedDateTime': parsingDateTime(e['modifiedDateTime'])
            })
        return pd.DataFrame(data)

    def customers(resp):
        data = []
        for e in resp:
            data.append({
                'id': e['id'],
                'no': e['no'],
                "name": e['name'],
                "city": e['city'],
                "ourAccountNo": e['ourAccountNo'],
                "territoryCode": e['territoryCode'],
                "globalDimension1Code": e['globalDimension1Code'],
                "globalDimension2Code": e['globalDimension2Code'],
                "chainName": e['chainName'],
                "salespersonCode": e['salespersonCode'],
                "countryRegionCode": e['countryRegionCode'],
                "comment": e['comment'],
                "blocked": e['blocked'],
                "paymentMethodCode": e['paymentMethodCode'],
                "lastModifiedDateTime": parsingDateTime(e['lastModifiedDateTime']),
                "salesLCY": e['salesLCY'],
                "profitLCY": e['profitLCY'],
                "balanceDueLCY": e['balanceDueLCY'],
                "paymentsLCY": e['paymentsLCY'],
                "invAmountsLCY": e['invAmountsLCY'],
                "crMemoAmountsLCY": e['crMemoAmountsLCY'],
                "outstandingOrdersLCY": e['outstandingOrdersLCY'],
                "outstandingInvoicesLCY": e['outstandingInvoicesLCY'],
                "noOfQuotes": e['noOfQuotes'],
                "noOfBlanketOrders": e['noOfBlanketOrders'],
                "noOfOrders": e['noOfOrders'],
                "noOfInvoices": e['noOfInvoices'],
                "noOfReturnOrders": e['noOfReturnOrders'],
                "noOfCreditMemos": e['noOfCreditMemos'],
                "lastInvoiced3PL": parsingDate(e['lastInvoiced3PL'])
            })

        return pd.DataFrame(data)

    def contacts(resp):
        data = []
        for e in resp:
            data.append({
                'id': e['id'],
                'systemModifiedAt': parsingDateTime(e['systemModifiedAt']),
                'no': e['no'],
                'type': e['type'],
                'name': e['name'],
                'searchName': e['searchName'],
                'city': e['city'],
                'eMail': e['eMail'],
                'companyNo': e['companyNo'],
                'companyName': e['companyName'],
                'customerNo3PL': e['customerNo3PL'],
                'vendorNo3PL': e['vendorNo3PL'],
                'statusCode3PL': e['statusCode3PL'],
            })

        return pd.DataFrame(data)

    def vendors(resp):
        data = []
        for e in resp:
            data.append({
                'id': e['id'],
                'no': e['no'],
                'name': e['name'],
                'city': e['city'],
                'ourAccountNo': e['ourAccountNo'],
                'territoryCode': e['territoryCode'],
                'globalDimension1Code': e['globalDimension1Code'],
                'globalDimension2Code': e['globalDimension2Code'],
                'budgetedAmount': e['budgetedAmount'],
                'shipmentMethodCode': e['shipmentMethodCode'],
                'priority': e['priority'],
                'lastModifiedDateTime': parsingDateTime(e['lastModifiedDateTime']),
                'balanceLCY': e['balanceLCY'],
                'invDiscountsLCY': e['invDiscountsLCY'],
                'pmtDiscountsLCY': e['pmtDiscountsLCY'],
                'balanceDueLCY': e['balanceDueLCY'],
                'invAmountsLCY': e['invAmountsLCY'],
                'outstandingOrders': e['outstandingOrders'],
                'reminderAmountsLCY': e['reminderAmountsLCY'],
                'outstandingInvoicesLCY': e['outstandingInvoicesLCY'],
                'noOfOrders': e['noOfOrders'],
                'noOfInvoices': e['noOfInvoices'],
                'noOfReturnOrders': e['noOfReturnOrders'],
                'noOfCreditMemos': e['noOfCreditMemos'],
                'statusCode3PL': e['statusCode3PL']
            })

        return pd.DataFrame(data)

    def purchaseHeaders(resp):
        data = []
        for e in resp:
            data.append({
                'id': e['id'],
                "documentType": e['documentType'],
                "no": e['no'],
                "payToVendorNo": e['payToVendorNo'],
                "payToName": e['payToName'],
                "yourReference": e['yourReference'],
                "orderDate": parsingDate(e['orderDate']),
                "postingDate": parsingDate(e['postingDate']),
                "purchaserCode": e['purchaserCode'],
                "amount": e['amount'],
                "amountIncludingVAT": e['amountIncludingVAT'],
                "vendorInvoiceNo": e['vendorInvoiceNo'],
                "documentDate": parsingDate(e['documentDate'])
            })

        return pd.DataFrame(data)

    def statusLog(resp):
        data = []
        for e in resp:
            data.append({
                'id': e['id'],
                'systemModifiedAt': parsingDateTime(e['systemModifiedAt']),
                'entryNo': e['entryNo'],
                'documentNo': e['documentNo'],
                'oldStatusCode': e['oldStatusCode'],
                'newStatusCode': e['newStatusCode'],
                'userID': e['userID'],
                'templateCode': e['templateCode']
            })

        return pd.DataFrame(data)


    def sample(resp):
        data = []
        for e in resp:
            data.append({
                'id': e['id'],
                'lastModifiedDateTime': pd.to_datetime(e['lastModifiedDateTime'])
            })
        return pd.DataFrame(data)


    def check(resp):
        data = []

        if 1 == 1:
            with open('kaas.json', 'w', encoding='utf-8') as f:
                json.dump(resp, f, indent=4, ensure_ascii=False)
            exit()

        for e in resp:
            data.append({
                'id': e['id'],
                'lastModifiedDateTime': pd.to_datetime(e['lastModifiedDateTime'])
            })
        return pd.DataFrame(data)

class Queries:

    def fillLedgerTableIsUtb():
        return \
    """
        UPDATE `mvl-sqldb-bi-prod`.generalLedgerEntries a
        LEFT JOIN (
            SELECT documentNo, lineNo, isUTB
            FROM `mvl-sqldb-bi-prod`.wmsDocumentLines
            
            UNION
            
            SELECT documentNo, lineNo, isUTB
            FROM `mvl-sqldb-bi-prod`.wmsPostedDocumentLines
        ) b
        ON a.wmsDocumentNo = b.documentNo
        AND a.wmsDocumentLineNo = b.lineNo

        SET a.isUTB = COALESCE(b.isUTB, 0)

        WHERE a.isUTB IS NULL;

    """