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
                'attribute05': e['attribute05']
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
                'wmsDocumentNo': e['wmsDocumentNo'],
                'wmsDocumentLineNo': e['wmsDocumentLineNo']
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
                json.dump(resp[:10], f, indent=4, ensure_ascii=False)
            exit()

        for e in resp:
            data.append({
                'id': e['id'],
                'lastModifiedDateTime': pd.to_datetime(e['lastModifiedDateTime'])
            })
        return pd.DataFrame(data)
