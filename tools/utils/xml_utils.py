import lxml.etree as ET

def modifyXml(path, modifyFunc, doctype):
	parser = ET.XMLParser(remove_blank_text=True)
	tree = ET.parse(path, parser)
	modifyFunc(tree)
	tree.write(path, pretty_print = True, xml_declaration = True, encoding = 'UTF-8', doctype = doctype)

def replaceElements(tree, path, newElements):
	oldElements = []
	parent = None
	index = 0

	for element in tree.iterfind(path):
		oldElements.append(element)
		parent = element.getparent()
		index = parent.index(element)

	for element in newElements:
		index += 1
		parent.insert(index, element)

	for element in oldElements:
		parent.remove(element)
