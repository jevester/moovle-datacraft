import ast

s = '{"documentType", "Document_Type"}, {"no", "No"}, {"sellToCustomerNo", "Sell_to_Customer_No"}, {"buildingCode", "Building_Code"}, {"postingDate", "Posting_Date"}, {"statusCode", "Status_Code"}, {"containerNo", "Container_No"}, {"vesselNo", "Vessel_No"}, {"senderAddressNo", "Sender_Address_No"}, {"shipToAddressNo", "Ship_to_Address_No"}, {"billOfLadingNo", "Bill_of_Lading_No"}, {"shortcutDimension2Code", "Shortcut_Dimension_2_Code"}, {"attribute04", "Attribute_04"}, {"containerSizeCode", "Container_Size_Code"}, {"sellToCustomerName", "Sell_to_Customer_Name"}, {"billToCustomerNo", "Bill_to_Customer_No"}, {"billToCustomerName", "Bill_to_Customer_Name"}, {"salespersonCode", "Salesperson_Code"}, {"direction", "Direction"}, {"locationNo", "Location_No"}, {"movementType", "Movement_Type"}, {"documentDate", "Document_Date"}, {"orderDate", "Order_Date"}, {"statusTemplateCode", "Status_Template_Code"}, {"createdDateTime", "Created_Date_Time"}, {"modifiedDateTime", "Modified_Date_Time"}, {"noSeries", "No_Series"}, {"externalDocumentNo", "External_Document_No"}, {"externalReference", "External_Reference"}, {"shippingAgentCode", "Shipping_Agent_Code"}, {"announcedDate", "Announced_Date"}, {"arrivedDate", "Arrived_Date"}, {"departedDate", "Departed_Date"}, {"deliveryDate", "Delivery_Date"}, {"estimatedDepartureDate", "Estimated_Departure_Date"}, {"parentDocumentType", "Parent_Document_Type"}, {"parentDocumentNo", "Parent_Document_No"}, {"customsCode", "Customs_Code"}, {"tariffNo", "Tariff_No"}, {"countryOfOriginCode", "Country_of_Origin_Code"}, {"countryOfDestinationCode", "Country_of_Destination_Code"}, {"declarationDate", "Declaration_Date"}, {"destination", "Destination"}, {"expectQtyCarriers", "Expect_Qty_Carriers"}, {"customsValue", "Customs_Value"}, {"incotermsCode", "Incoterms_Code"}, {"invoiceValue", "Invoice_Value"}, {"incotermsCity", "Incoterms_City"}, {"additionalDocumentNo", "Additional_Document_No"}, {"certificateNo", "Certificate_No"}, {"shortcutDimension1Code", "Shortcut_Dimension_1_Code"}, {"attribute05", "Attribute_05"}, {"grossWeight", "Gross_Weight"}, {"netWeight", "Net_Weight"}, {"quantity", "Quantity"}, {"carrierQuantity", "Carrier_Quantity"}, {"portFrom", "Port_From"}, {"portTo", "Port_To"}, {"sealNo", "Seal_No"}, {"sendersAddressName", "Senders_Address_Name"}, {"shipToAddressName", "Ship_to_Address_Name"}, {"sendersAddressCity", "Senders_Address_City"}, {"shipToAddressCity", "Ship_to_Address_City"}, {"carrierQtyCreated", "Carrier_Qty_Created"}, {"consigneeName", "Consignee_Name"}, {"shipperName", "Shipper_Name"}, {"agentName", "Agent_Name"}'


s = s.replace('{', '(').replace('}', ')')
d = f"[{s}]"
d = ast.literal_eval(d)


l = []

for (a, b) in d:
    print(a)
    l.append(a)
    # exit()

print(l)